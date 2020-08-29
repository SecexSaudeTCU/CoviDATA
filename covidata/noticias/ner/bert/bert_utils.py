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
            if tags:
                tag_docs.append(tags[indices_sublistas[i]:indices_sublistas[i + 1]])
        else:
            token_docs.append(tokens[indices_sublistas[i]:])
            if tags:
                tag_docs.append(tags[indices_sublistas[i]:])

    return token_docs, tag_docs