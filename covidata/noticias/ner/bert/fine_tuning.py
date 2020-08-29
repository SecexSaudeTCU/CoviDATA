import re
from operator import itemgetter
from typing import Dict, Tuple, List

import jsonlines
import numpy as np
import torch
from seqeval.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader
from transformers import BertModel, DistilBertTokenizerFast, TrainingArguments, Trainer, \
    DistilBertForTokenClassification, BertForTokenClassification, AdamW, EvalPrediction
from torch import nn

from covidata import config
import logging

logging.basicConfig(level=logging.INFO)


class NoticiasDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)


def treinar():
    unique_tags, train_dataset, val_dataset, tokenizer, id2tag = processar_base()
    model = BertForTokenClassification.from_pretrained(config.subdiretorio_modelo_neuralmind_bert_base,
                                                       num_labels=len(unique_tags))

    training_args = TrainingArguments(
        output_dir='./results',  # output directory
        num_train_epochs=3,  # total number of training epochs
        # per_device_train_batch_size=16,  # batch size per device during training
        # per_device_eval_batch_size=64,  # batch size for evaluation
        per_device_train_batch_size=1,  # batch size per device during training
        per_device_eval_batch_size=1,  # batch size for evaluation
        warmup_steps=500,  # number of warmup steps for learning rate scheduler
        weight_decay=0.01,  # strength of weight decay
        #logging_dir='./logs',  # directory for storing logs
        logging_steps=10,
        evaluate_during_training=True,
    )

    def align_predictions(predictions: np.ndarray, label_ids: np.ndarray) -> Tuple[List[int], List[int]]:
        preds = np.argmax(predictions, axis=2)
        batch_size, seq_len = preds.shape
        out_label_list = [[] for _ in range(batch_size)]
        preds_list = [[] for _ in range(batch_size)]

        for i in range(batch_size):
            for j in range(seq_len):
                if label_ids[i, j] != nn.CrossEntropyLoss().ignore_index:
                    out_label_list[i].append(id2tag[label_ids[i][j]])
                    preds_list[i].append(id2tag[preds[i][j]])

        return preds_list, out_label_list

    def compute_metrics(p: EvalPrediction) -> Dict:
        preds_list, out_label_list = align_predictions(p.predictions, p.label_ids)
        return {
            "accuracy_score": accuracy_score(out_label_list, preds_list),
            "precision": precision_score(out_label_list, preds_list),
            "recall": recall_score(out_label_list, preds_list),
            "f1": f1_score(out_label_list, preds_list),
        }

    trainer = Trainer(
        model=model,  # the instantiated 🤗 Transformers model to be trained
        args=training_args,  # training arguments, defined above
        train_dataset=train_dataset,  # training dataset
        eval_dataset=val_dataset,  # evaluation dataset,
        compute_metrics=compute_metrics
    )

    trainer.train()


def processar_base():
    tokenizer = DistilBertTokenizerFast.from_pretrained('neuralmind/bert-base-portuguese-cased'
                                                        , model_max_length=512
                                                        , do_lower_case=False
                                                        )
    # Obtém a base rotulada a partir de um arquivo JSONL gerado pela ferramenta de anotação Docanno
    textos, tags = get_textos_tags()
    textos, tags = pre_processar_base(textos, tags, tokenizer)

    # Divide os textos com quantidade de tokens maior do que o suportado em textos menores.

    train_texts, val_texts, train_tags, val_tags = train_test_split(textos, tags, test_size=.2, random_state=42)

    unique_tags = set(tag for doc in tags for tag in doc)

    tag2id = {tag: id for id, tag in enumerate(unique_tags)}
    id2tag = {id: tag for tag, id in tag2id.items()}

    train_encodings = tokenizer(train_texts, is_pretokenized=True, return_offsets_mapping=True, padding=True,
                                truncation=True)
    val_encodings = tokenizer(val_texts, is_pretokenized=True, return_offsets_mapping=True, padding=True,
                              truncation=True)

    train_labels = encode_tags(train_tags, train_encodings, tag2id, tokenizer, train_texts)
    val_labels = encode_tags(val_tags, val_encodings, tag2id, tokenizer, val_texts)

    train_encodings.pop("offset_mapping")  # we don't want to pass this to the model
    val_encodings.pop("offset_mapping")
    train_dataset = NoticiasDataset(train_encodings, train_labels)
    val_dataset = NoticiasDataset(val_encodings, val_labels)

    return unique_tags, train_dataset, val_dataset, tokenizer, id2tag


def encode_tags(tags, encodings, tag2id, tokenizer, textos):
    labels = [[tag2id[tag] for tag in doc] for doc in tags]
    encoded_labels = []

    for i, (doc_labels, doc_offset) in enumerate(zip(labels, encodings.offset_mapping)):
        # create an empty array of -100
        doc_enc_labels = np.ones(len(doc_offset), dtype=int) * -100
        arr_offset = np.array(doc_offset)

        # set labels whose first offset position is 0 and the second is not 0
        doc_enc_labels[(arr_offset[:, 0] == 0) & (arr_offset[:, 1] != 0)] = doc_labels
        encoded_labels.append(doc_enc_labels.tolist())

    return encoded_labels


def pre_processar_base(textos, tags, tokenizer):
    max_len = tokenizer.max_len - tokenizer.num_special_tokens_to_add()
    nova_lista_textos = []
    nova_lista_tags = []

    for i in range(0, len(textos)):
        tokens = textos[i]
        ts = tags[i]
        token_docs, tag_docs = pre_processar_texto(tokens, ts, tokenizer, max_len)
        nova_lista_textos += token_docs
        nova_lista_tags += tag_docs

    return nova_lista_textos, nova_lista_tags


def pre_processar_texto(tokens, tags, tokenizer, max_len):
    subword_len_counter = 0
    indices_sublistas = [0]
    token_docs = []
    tag_docs = []

    for i, token in enumerate(tokens):
        current_subwords_len = len(tokenizer.tokenize(token))

        # Filtra caracteres especiais
        if current_subwords_len == 0:
            continue

        if (subword_len_counter + current_subwords_len) >= max_len:
            indices_sublistas.append(i)
            subword_len_counter = current_subwords_len
        else:
            subword_len_counter += current_subwords_len

    for i in range(0, len(indices_sublistas)):
        if i + 1 < len(indices_sublistas):
            sublista = tokens[indices_sublistas[i]:indices_sublistas[i + 1]]
            token_docs.append(sublista)
            tag_docs.append(tags[indices_sublistas[i]:indices_sublistas[i + 1]])
        else:
            token_docs.append(tokens[indices_sublistas[i]:])
            tag_docs.append(tags[indices_sublistas[i]:])

    return token_docs, tag_docs


def get_textos_tags():
    token_docs = []
    tag_docs = []

    with jsonlines.open(config.arquivo_noticias_rotulado) as reader:
        for obj in reader:
            texto = obj['text']
            labels = obj['labels']
            labels = sorted(labels, key=itemgetter(0))
            tokens = re.findall(r"[\w']+|[-.,!?;()]", texto)

            # Ignora aspas
            tokens = [token.replace('\'', '') for token in tokens]

            tags = ['O'] * len(tokens)
            indice = 0

            for inicio, fim, label in labels:
                trecho = texto[inicio:fim]
                termos_trecho = re.findall(r"[\w']+|[-.,!?;()]", trecho)

                # Ignora aspas
                termos_trecho = [termo_trecho.replace('\'', '') for termo_trecho in termos_trecho]

                inicio_trecho = tokens.index(termos_trecho[0], indice)
                tags[inicio_trecho] = 'B-' + label

                if len(termos_trecho) > 1:
                    fim_trecho = tokens.index(termos_trecho[len(termos_trecho) - 1], inicio_trecho)

                    if fim_trecho != inicio_trecho:
                        tags[fim_trecho] = 'L-' + label

                    for i in range(inicio_trecho + 1, fim_trecho):
                        tags[i] = 'I-' + label
                else:
                    fim_trecho = inicio_trecho

                indice = max(inicio_trecho, fim_trecho) + 1

            token_docs.append(tokens)
            tag_docs.append(tags)

    return token_docs, tag_docs


treinar()
