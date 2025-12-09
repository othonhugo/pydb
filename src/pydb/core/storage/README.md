# `storage`

O pacote `storage` é a camada mais fundamental do banco de dados. Sua única responsabilidade é a **persistência física dos dados**. Ele fornece uma abstração para escrever, ler e remover blocos de dados de um meio de armazenamento, como um arquivo em disco.

Este pacote é o "motor" do banco de dados. Ele é intencionalmente "burro" sobre o significado dos dados que armazena: ele apenas lida com chaves e valores como sequências de bytes (`bytes`). Em outras palavras, ele não sabe nada sobre esquemas, tabelas, usuários ou permissões...
