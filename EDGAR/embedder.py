


class embedder:
    def __init__(self):
        pass
        
    def _embed_word(self, word: str, method='onehot'):
        if method == 'onehot':
            #Return one-hot index of associated token
            return self.vocab.index(word)
        pass;

    def embed_sentence(self, sentence: str):
        """
        tokenize sentence;
        for each token in sentence:
            _embed_word
        """
        pass;

    def generate_vocab(self, corpus: list[str]):
        vocab = list()
        for sentence in corpus:
            for word in tokenized(sentence):
                vocab.append(word)
        vocab = np.unique(vocab)
        self.vocab = vocab
        
    def _save_vocab(self):
        pass;
        
    def _load_vocab(self):
        pass;
                
            
    