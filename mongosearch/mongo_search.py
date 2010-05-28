# −*− coding: UTF−8 −*−
"""
This is the python wrapper to the mongo full text search javascript. As such it
has to implement OR CALL all the mapreduce invocations in the javascript
library, plus anything else that we do not wish to block the server upon
executing. That is:

search.mapReduceIndex
search.mapReduceTermScore
search.mapReduceSearch
search.mapReduceNiceSearch
search.stemAndTokenize (and thus search.stem and search.tokenizeBasic )

Optional (if you don't mind calling blocking execution server-wide)
search.processQueryString
search.encodeQueryString
"""
import re

import pymongo
from pymongo.code import Code

import util
import porter

TOKENIZE_BASIC_RE = re.compile(r"\b(\w[\w'-]*\w|\w)\b") #this should match the RE in use on the server
INDEX_NAMESPACE = 'search_.indexes'

def ensure_text_index(collection):
    """Execute all relevant bulk indexing functions
    ie:
        mapReduceIndex , which extracts indexed terms and puts them in a new collection
        mapReduceTermScore , which creates a table of scores for each term.
    which is covered by mapReduceIndexTheLot
    """
    return util.exec_js_from_string(
      "mft.get('search').mapReduceIndexTheLot('%s');" % collection.name,
      collection.database)

def search(collection, search_query_string):
    """
    Re-implmentation of JS function search.mapReduceSearch
    """
    search_query_terms = process_query_string(search_query_string)
    map_js = Code("function() { mft.get('search')._searchMap.call(this) }")
    reduce_js = Code("function(k, v) { return mft.get('search')._searchReduce(k, v) }")
    scope =  {'search_terms': search_query_terms, 'coll_name': collection.name}
    #   lazily assuming "$all" (i.e. AND search) 
    query_obj = {'value._extracted_terms': {'$all': search_query_terms}}
    db = collection.database
    res = db[index_name(collection)].map_reduce(
      map_js, reduce_js, scope=scope, query=query_obj)
    res.ensure_index([('value.score', pymongo.ASCENDING)]) # can't demand backgrounding in python seemingly?
    # should we be returning a verbose result, or just the collection here?
    return res

def _query_obj_for_terms(search_query_terms):
    return {'value._extracted_terms': {'$all': search_query_terms}}
    
def nice_search_by_query(collection, search_query_string, query_obj):
    """
    Search, returning full result sets and limiting by the supplied id_list
    A re-implementation of the javascript function search.mapReduceNiceSearch.
    """
    # because we only have access to the index collection later, we have to convert 
    # the query_obj to an id list
    id_list = [rec['_id'] for rec in collection.find(query_obj, ['_id'])]
    return nice_search_by_ids(collection, search_query_string, id_list)

def nice_search_by_ids(collection, search_query_string, id_list=None):
    """
    Search, returning full result sets and limiting by the supplied id_list
    """
    raw_search_results = search(collection, search_query_string)
    search_coll_name = raw_search_results.name
    map_js = Code("function() { mft.get('search')._niceSearchMap.call(this) }")
    reduce_js = Code("function(k, v) { return mft.get('search')._niceSearchReduce(k, v) }")
    scope =  {'coll_name': collection.name}
    db = collection.database
    sorting = {'value.score': pymongo.DESCENDING}
    if id_list is None:
        id_query_obj = {}
    else:
        id_query_obj = {'_id': {'$in': id_list}}
    res_coll = db[search_coll_name].map_reduce(map_js, reduce_js, 
        query=id_query_obj, scope=scope, sort=sorting)
    #should we be ensuring an index here? or just leave it?
    # res_coll.ensure_index([('value.score', pymongo.ASCENDING)])
    return res_coll.find()

def nice_search(collection, search_query_string):
    return nice_search_by_ids(collection, search_query_string, None)
    
def process_query_string(query_string):
    return sorted(stem_and_tokenize(query_string))

def stem_and_tokenize(phrase):
    return stem(tokenize(phrase.lower()))

def stem(tokens):
    """
    now we could do this in python. We coudl also call the same function that
    exists server-side, or even run an embedded javascript interpreter. See
    http://groups.google.com/group/mongodb-user/browse_frm/thread/728c4376c3013007/b5ac548f70c8b3ca
    """
    return [porter.stem(tok) for tok in tokens]

def tokenize(phrase):
    return [m.group(0) for m in TOKENIZE_BASIC_RE.finditer(phrase)]

def index_name(collection):
    return INDEX_NAMESPACE + '.' + collection.name
    
class TextIndexedCollection(object):
    """
    Wrap a pymongo.collections.Collection and provide full-text search functions
    """
    def __init__(self, collection, *args, **kwargs):
        self.collection = collection
    def __getattr__(self, att):
        return getattr(self.collection, att)

    ensure_text_index = ensure_text_index
    _search = search
    
    def search_by_ids(self, search_query_string, id_list, limit=None, skip=None):
        return SearchCursor(self, search_query_string, id_list=id_list, limit=limit, skip=skip)
    
    def search_by_query(self, search_query_string, spec, limit=None, skip=None):
        return SearchCursor(self, search_query_string, spec=spec, limit=limit, skip=skip)
    
    def search(self, search_query_string, limit=None, skip=None):
        return SearchCursor(self, search_query_string, limit=limit, skip=skip)


class SearchCursor(object):
    def __init__(self, collection, search_query_string, id_list=None, spec=None, limit=None, skip=None):
        self.collection = collection
        self.search_query_string = search_query_string
        self._id_list = id_list
        self._spec = spec
        self._actual_result_cursor = None
        self._limit = limit
        self._skip = skip

    def _cached_result_cursor(self):
        if self._actual_result_cursor is None:
            self._perform_search()
        return self._actual_result_cursor
    
    def __iter__(self):
        for wrapped_rec in self._cached_result_cursor():
            yield wrapped_rec['value']
        
    def __getitem__(self, item):
        return self._cached_result_cursor()[item]['value']
    
    def rewind(self):
        if self._actual_result_cursor is not None:
            self._actual_result_cursor.rewind()
        return self
    
    def limit(self, limit):
        # could do much optimising here
        if self._actual_result_cursor is not None:
            raise InvalidOperation("Cannot set search options after executing SearchQuery")
        self._limit = limit
        return self
    
    def skip(self, skip):
        if self._actual_result_cursor is not None:
            raise InvalidOperation("Cannot set search options after executing SearchQuery")
        self._skip = skip
        return self
    
    def count(self):
        # if we haven't done the query yet, don't do a full search - just minimum to get the count right
        if self._actual_result_cursor is None \
          or self._limit is not None or self.skip is not None:
            db = self.collection.database
            index_coll = db[index_name(self.collection)]
            #shoudl refactor this by moving search() inside this cursor, so we can cache this stuff
            return index_coll.find(_query_obj_for_terms(process_query_string(search_query_string))).count()
        else:
            return self._actual_result_cursor.count()
    
    def _perform_search(self):
        raw_search_results = self.collection._search(self.search_query_string)
        search_coll_name = raw_search_results.name
        map_js = Code("function() { mft.get('search')._niceSearchMap.call(this) }")
        reduce_js = Code("function(k, v) { return mft.get('search')._niceSearchReduce(k, v) }")
        scope =  {'coll_name': self.collection.name}
        db = self.collection.database
        sorting = {'value.score': pymongo.DESCENDING}
        id_list = self.id_list()
        id_query_obj = {} if id_list is None else {'_id': {'$in': id_list}}
        self._actual_result_cursor = db[search_coll_name].map_reduce(map_js, reduce_js, 
            query=id_query_obj, scope=scope, sort=sorting).find()
        self._actual_result_cursor.sort([('value.score', pymongo.DESCENDING)])
        if self._limit is not None: #optimise the hell out of this later maybe?
            self._actual_result_cursor.limit(self._limit)
        if self._skip is not None:
            self._actual_result_cursor.skip(self._skip)
        #should we be ensuring an index here? or just leave it?
        # res_coll.ensure_index([('value.score', pymongo.ASCENDING)])
    
    def id_list(self):
        if self._id_list is not None:
            return self._id_list
        elif self._spec is not None:
            return [rec['_id'] for rec in collection.find(self._spec, ['_id'])]
        else:
            return None
        
        
        