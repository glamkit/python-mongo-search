# −*− coding: UTF−8 −*−
"""
This is the python wrapper to the mongo full text search javascript. As such it
has to implement OR CALL all the mapreduce invocations in the javascript
library, plus anything else that we do not wish to block the server upon
executing. That is:

search.mapReduceIndex
search.mapReduceTermScore
search.mapReduceRawSearch
search.mapReduceSearch
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
CONFIG_COLLECTION = 'search_.config'
DEFAULT_INDEX_NAME = 'default_'

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

def configure_text_index_fields(collection, fields, index_name=None):
    """
    Configure the text search index named `index_name` on the supplied `collection`.
    
    `fields_json` should be dict containing an array
    with fieldnames as keys and integers as values -- eg:
        '{content: 1, title: 5}'
    """
    if index_name is None:
        index_name = DEFAULT_INDEX_NAME
    # do some basic validation here, to try and catch errors that might ocur in the JS
    if not isinstance(fields, dict):
        raise InvalidSearchFieldConfiguration("Fields must be a dictionary of fieldname/weighting pairs")
    for fieldname, fieldvalue in fields.iteritems():
        if not isinstance(fieldname, str) and not isinstance(fieldname, unicode):
            raise InvalidSearchFieldConfiguration("Field names (the keys of the `fields` dict)"
                "must be strings or unicode objects. You supplied %r of type %s" % (fieldname, type(fieldname)))
        if not isinstance(fieldvalue, int):
            raise InvalidSearchFieldConfiguration("Field value (the keys of the `fields` dict)"
                "must be integers. You supplied %r of type %s" % (fieldvalue, type(fieldvalue)))
    fields_bson = pymongo.bson.BSON(fields)
    # TODO: should maybe jsut put this direct into the DB?
    output = util.exec_js_from_string(
      'mft.get("search").configureSearchIndexFields("%s", %s, "%s");' % 
        (collection.name, fields_bson, index_name), collection.database)
    return output
    
    
def raw_search(collection, search_query):
    """
    Re-implmentation of JS function search.mapReduceRawSearch
    """
    #TODO: add in a spec param here - we may as well pre-filter this search too
    # this means we can also then sort the output and just use relevant ones later
    # when we have to do limit etc.
    search_query_terms = process_query_string(search_query)
    index_name = DEFAULT_INDEX_NAME # assuem this for now -this is a legacy interface we can sdelete soon.
    map_js = Code("function() { mft.get('search')._rawSearchMap.call(this) }")
    reduce_js = Code("function(k, v) { return mft.get('search')._rawSearchReduce(k, v) }")
    scope =  {'search_terms': search_query_terms, 'coll_name': collection.name, 'index_name': index_name}
    #   lazily assuming "$all" (i.e. AND search) 
    query_obj = {'value._extracted_terms': {'$all': search_query_terms}}
    db = collection.database
    res = db[index_coll_name(collection, index_name)].map_reduce(
      map_js, reduce_js, scope=scope, query=query_obj)
    res.ensure_index([('value.score', pymongo.ASCENDING)]) # can't demand backgrounding in python seemingly?
    # should we be returning a verbose result, or just the collection here?
    return res

def _query_obj_for_terms(search_query_terms):
    return {'value._extracted_terms': {'$all': search_query_terms}}
    
def search_by_query(collection, search_query_string, query_obj):
    """
    Search, returning full result sets and limiting by the supplied id_list
    A re-implementation of the javascript function search.mapReduceSearch.
    """
    # because we only have access to the index collection later, we have to convert 
    # the query_obj to an id list
    id_list = [rec['_id'] for rec in collection.find(query_obj, ['_id'])]
    return search_by_ids(collection, search_query_string, id_list)

def search_by_ids(collection, search_query_string, id_list=None):
    """
    Search, returning full result sets and limiting by the supplied id_list
    """
    raw_search_results = raw_search(collection, search_query_string)
    search_coll_name = raw_search_results.name
    map_js = Code("function() { mft.get('search')._searchMap.call(this) }")
    reduce_js = Code("function(k, v) { return mft.get('search')._searchReduce(k, v) }")
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

def search(collection, search_query_string):
    return search_by_ids(collection, search_query_string, None)
    
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

def index_coll_name(collection, index_name):
    return INDEX_NAMESPACE + '.' + collection.name + '.' + index_name
    
class SearchableCollection(object):
    """
    Wrap a pymongo.search_collections.Collection and provide full-text search functions
    """
    def __init__(self, collection, *args, **kwargs):
        self.search_collection = collection
    def __getattr__(self, att):
        return getattr(self.search_collection, att)

    ensure_text_index = ensure_text_index
    configure_text_index_fields = configure_text_index_fields
    
    def get_configuration(self):
        return self.search_collection.database[CONFIG_COLLECTION].find_one({'collection_name': self.search_collection.name})
    
    def search(self, search_query, spec=None, id_list=None, limit=None, skip=None):
        """Search for the specified `search_query` in this collection.
        
        `search_query` can be a string, which will search in the default index named DEFAULT_INDEX_NAME, or
        a dictionary, where the key indicates which named index to search in. Currently
        searching through multiple indexes is not supported.
        
        `spec` prefilters the search results with the given query object, operating the same way
        as the same argument to .find() on a regular cursor.
        `id_list` is a list of values for `_id` which you want to restric the search to. If you know
        the id_list already, it is more efficient to supply that than `spec`, as
        the latter is converted to an id_list behind the scenes to make it compatible with MapReduce.
        `limit` and `skip` have the same meaning as the arguments to .find()
        """
        return SearchCursor(self, search_query, spec=spec, id_list=id_list, limit=limit, skip=skip)


class SearchCursor(object):
    """A cursor to iterate through search results. Should not be instantiated directly, but returned by
    calling SearchableCollection.search().
    """
    def __init__(self, search_collection, search_query, id_list=None, spec=None, limit=0, skip=0):
        if id_list and spec:
            raise InvalidSearchOperation("Can't set id_list and spec at the same time")
        self.search_collection = search_collection
        if isinstance(search_query, dict): #eww, not very pythonic, any ideas here?
            if len(search_query) > 1 or len(search_query) == 0:
                raise InvalidSearchOperation("Number of indexes requested must be exactly one")
            self.search_index_name = search_query.keys()[0]
            self.search_query_string = search_query[self.search_index_name]
            #Should we check if it's a valid index here
        else:
            self.search_query_string = search_query
            self.search_index_name = DEFAULT_INDEX_NAME 
        self.search_query_terms = process_query_string(self.search_query_string)
        self._id_list = id_list
        self._spec = spec
        self._actual_result_cursor = None
        self._limit = limit
        self._skip = skip
        self._get_search_idx_collection() #throw an error now for invalid index

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
        """Limit the search to supplied number of results.
        
        This is useful for pagination. This operated the same way as .limit() on a regular cursor.
        """
        if self._actual_result_cursor is not None:
            raise InvalidSearchOperation("Cannot set search options after executing SearchQuery")
        self._limit = limit
        return self
    
    def skip(self, skip):
        """Skip the supplied number of results in the result output
        
        This is useful for pagination. This operated the same way as .skip() on a regular cursor.
        """
        if self._actual_result_cursor is not None:
            raise InvalidSearchOperation("Cannot set search options after executing SearchQuery")
        self._skip = skip
        return self
    
    def count(self):
        # if we haven't done the query yet, don't do a full search - just minimum to get the count right
        if self._actual_result_cursor is None \
          or self._limit is not None or self.skip is not None:
            #shoudl refactor this by moving search() inside this cursor, so we can cache this stuff
            return self._get_search_idx_collection().find(_query_obj_for_terms(self.search_query_terms)).count()
        else:
            return self._actual_result_cursor.count()
    
    def _perform_search(self):
        self._raw_search()
        search_coll_name = self._raw_result_coll.name
        map_js = Code("function() { mft.get('search')._searchMap.call(this) }")
        reduce_js = Code("function(k, v) { return mft.get('search')._searchReduce(k, v) }")
        scope =  {'coll_name': self.search_collection.name}
        db = self.search_collection.database
        # sorting = [('value.score', pymongo.DESCENDING)]    #Seems to not make any difference?
        if self._limit or self._skip: 
            # avoid instantiating extra objects by sorting on the raw resutls first
            # so if only need 20 actual objects, we can get them only
            raw_result_cursor = self._raw_result_coll.find(fields=['_id']).sort(
              [('value.score', pymongo.DESCENDING)])
            if self._limit:
                raw_result_cursor.limit(self._limit)
            if self._skip:
                raw_result_cursor.skip(self._skip)
            id_list = [rec['_id'] for rec in raw_result_cursor]
            id_query_obj = {'_id': {'$in': id_list}}
        else:
            id_query_obj = None
        self._actual_result_cursor = db[search_coll_name].map_reduce(map_js, reduce_js, 
            query=id_query_obj, scope=scope).find()
        self._actual_result_cursor.sort([('value.score', pymongo.DESCENDING)])
        #should we be ensuring an index here? or just leave it?
        # res_coll.ensure_index([('value.score', pymongo.ASCENDING)])
    
    def _raw_search(self):
        map_js = Code("function() { mft.get('search')._rawSearchMap.call(this) }")
        reduce_js = Code("function(k, v) { return mft.get('search')._rawSearchReduce(k, v) }")
        scope =  {'search_terms': self.search_query_terms, 'coll_name': self.search_collection.name, 
          'index_name': self.search_index_name}
        #   lazily assuming "$all" (i.e. AND search) 
        query_obj = {'value._extracted_terms': {'$all': self.search_query_terms}}
        id_list = self.id_list()
        if id_list is not None:
            query_obj['_id'] = {'$in': id_list}
        self._raw_result_coll = self._get_search_idx_collection().map_reduce(
          map_js, reduce_js, scope=scope, query=query_obj)
        self._raw_result_coll.ensure_index([('value.score', pymongo.ASCENDING)]) 
        # can't demand backgrounding in python seemingly?
    
    def id_list(self):
        if self._id_list is not None:
            return self._id_list
        elif self._spec is not None:
            return [rec['_id'] for rec in self.search_collection.find(self._spec, ['_id'])]
        else:
            return None
    
    def _get_search_idx_collection(self):
        db = self.search_collection.database
        name_for_index_coll = index_coll_name(self.search_collection, self.search_index_name)
        if name_for_index_coll not in db.collection_names():
            if self._get_search_idx_config() is None:
                raise SearchIndexNotConfiguredException("Search index '%s' does not exist"
                    " as index name '%s' has not been configured" % (
                    name_for_index_coll, self.search_index_name))
            # TODO: this should distinguish between the unindexed case and the missing config item case
            # would be a simple matter of checking the DB config
            raise SearchIndexNotInitializedException("Search index '%s' does not exist because"
                " the database hasn't been indexed for requested index name '%s'" % (
                name_for_index_coll, self.search_index_name))
        return db[name_for_index_coll]
        
    def _get_search_idx_config(self):
        all_index_config = self.search_collection.get_configuration()
        if not all_index_config:
            return None
        try:
            return all_index_config['indexes'][self.search_index_name]
        except KeyError:
            return None
        
class InvalidSearchOperation(pymongo.errors.InvalidOperation):
    pass

class MissingSearchIndexException(InvalidSearchOperation):
    pass
    
class SearchIndexNotConfiguredException(MissingSearchIndexException):
    pass
    
class SearchIndexNotInitializedException(MissingSearchIndexException):
    pass

class InvalidSearchFieldConfiguration(InvalidSearchOperation):
    pass