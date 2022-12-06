
import os
import pickle as pkl
from yaml import load, CLoader as Loader, dump, CDumper as Dumper
import warnings


class metadata_manager(dict):
    def __init__(self, data_dir='data', *arg, **kw):
        super(metadata_manager, self).__init__(*arg, **kw)
        
        # Always gets the path of the current file
        self.path = os.path.abspath(os.path.join(__file__, os.pardir))
        
        self.meta_dir = os.path.join(self.path, data_dir, 'metadata')
        if not os.path.exists(self.meta_dir):
            os.system('mkdir -p ' + self.meta_dir)
        
        # Used by dataloader for API
        self.keys_path = os.path.join(self.path, data_dir, 'metadata', '.keys.yaml')
        self.keys = None
        
    def load_keys(self):

        if not os.path.exists(self.keys_path):
            warnings.warn("No .keys.yaml located", RuntimeWarning)
            self.keys = dict()
            return;
        self.keys = load(open(self.keys_path, 'r'), Loader=Loader)
    
    def save_keys(self):
        
        dump(self.keys, open(self.keys_path, 'w'), Dumper=Dumper) 
    
    def load_tikr_metadata(self, tikr):

        data_path = os.path.join(self.meta_dir, f"{tikr}.pkl")

        if os.path.exists(data_path):
        
            with open(data_path, 'rb') as f:
                self[tikr] = pkl.load(f)
            return True

        self.initialize_tikr_metadata(tikr);

        return False

    def save_tikr_metadata(self, tikr):

        self.initialize_tikr_metadata(tikr)

        data_path = os.path.join(self.meta_dir, f"{tikr}.pkl")

        with open(data_path, 'wb') as f:
            pkl.dump(self.get(tikr), f)

    def initialize_tikr_metadata(self, tikr):
        if tikr not in self:
            self[tikr] = {'attrs': dict(), 'submissions': dict()}
                
    def initialize_submission_metadata(self, tikr, fname):
        pdict = self[tikr]['submissions']
        if fname not in pdict:
            pdict[fname] = {'attrs': dict(), 'documents': dict()}
    
    def get_10q_name(self, tikr, submission):
        """
        Parameters
        ---------
        tikr: str
            a company identifier to query
        submission:
            the associated company filing for which to find a 10-Q form
            

        Returns
        --------
        filename: str
            The name of the 10-q file associated with the submission, or None
        """
        meta = self[tikr]['submissions'][submission]['documents']
        for file in meta:
            if meta[file]['type'] in ['10-Q', 'FORM 10-Q']:
                return meta[file]['filename']
        return None

    def get_submissions(self, tikr: str, *, only_annotated: bool = False, only_unannotated: bool = False):
        """
        Parameters
        ---------
        tikr: str
            a company identifier to query
        only_annotated: str, optional
            if 'True', then only returns documents with iXBRL annotations
        only_unannotated: str, optional
            if 'True', then only returns documents without iXBRL annotations. Mutually exclusive with only_annotated

        Returns
        --------
        submissions: list
            a list of string names of filing submissions under the company tikr

        """
        if only_annotated and only_unannotated:
            raise RuntimeError('Set mutually exclusive arguments')
        
        if 'submissions' in self[tikr]:
            if only_annotated:
                return [i for i in self[tikr]['submissions'] if self._is_10q_annotated(tikr, i)]
            if only_unannotated:
                return [i for i in self[tikr]['submissions'] if not self._is_10q_annotated(tikr, i)]
            return [i for i in self[tikr]['submissions']]
        return None
    
    """
        Returns whether given tikr submission has annotated ix elements
    """
    def _is_10q_annotated(self, tikr, submission) -> bool:
        """
        Parameters
        ---------
        tikr: str
            a company identifier to query
        submission: str
            an SEC filing to query

        Returns
        --------
        submissions: list
            a list of string names of filing submissions under the company tikr

        """

        assert tikr in self
        assert submission in self[tikr]['submissions']

        is_annotated = self[tikr]['submissions'][submission]['attrs'].get('is_10q_annotated', None)
        if is_annotated is not None:
            return is_annotated
        else:
            return self._gen_10q_annotated_metadata(tikr, submission)

    def _gen_10q_annotated_metadata(self, tikr: str, submission: str):

        annotated_tag_list = {'ix:nonnumeric','ix:nonfraction'}

        _file = None
        files = self[tikr]['submissions'][submission]['documents']
        for file in files:
            if files[file]['type'] == '10-Q':
                _file = files[file]['filename']

        # TODO handle ims-document
        if _file is None:
            warnings.warn("Document Encountered without 10-Q", RuntimeWarning)
            for file in files:
                if files[file].get('is_ims-document', False):
                    self.metadata[tikr]['submissions'][submission]['attrs']['is_10q_annotated'] = False
                    warnings.warn("Encountered unlabeled IMS-DOCUMENT", RuntimeWarning)
                    return False
            if len(files) == 0:
                warnings.warn("No Files under Document Submission", RuntimeWarning)
                return False

        assert _file is not None, 'Missing 10-Q'

        data = None
        fname = os.path.join(self.data_dir, 'processed', tikr, submission, _file)
        with open(fname, 'r') as f:
            data = f.read();
        for tag in annotated_tag_list:
            if re.search(tag, data):
                self.metadata[tikr]['submissions'][submission]['attrs']['is_10q_annotated'] = True
                return True
        self.metadata[tikr]['submissions'][submission]['attrs']['is_10q_annotated'] = False
        return False




    def find_sequence_of_file(self, tikr: str, submission: str, filename: str):
        level = self[tikr]['submissions'][submission]['documents']
        for sequence in level:
            if level[sequence]['filename'] == filename:
                return sequence
        return None
            
    def file_set_processed(self, tikr: str, submission: str, filename: str, val: bool):
        sequence = self.find_sequence_of_file(tikr, submission, filename)
        assert sequence is not None, "Error: filename not found"
        self[tikr]['submissions'][submission]['documents'][sequence]['features_pregenerated'] = val

    def file_was_processed(self, tikr: str, submission: str, filename: str):
        sequence = self.find_sequence_of_file(tikr, submission, filename)
        assert sequence is not None, "Error: filename not found"
        return self[tikr]['submissions'][submission]['documents'][sequence].get('features_pregenerated', False)
