# −*− coding: UTF−8 −*−
"""
actual tests for the mongo-full-text-search 
"""

from nose import with_setup
from nose.tools import assert_true, assert_equals, assert_raises
from mongosearch import mongo_search, util
import time
import sys

_daemon = None
_settings = {
    'dbpath': util.MongoDaemon.TEST_DIR, #i.e. a temporary folder, system-wide
    'port': 29017,
    'host': '127.0.0.1',
    'network_timeout': 5
}
_connection = None
_database = None
_collection = None

def setup_module():
    """
    Instantiate a new mongo daemon and corresponding connection and 
    then insert the appropriate test fixture.
    """
    from pymongo.connection import AutoReconnect
    import time
    daemon = _setup_daemon()
    conn_tries = 0
    while True:
        try:
            _connection = util.get_connection(**_settings)
            break
        except AutoReconnect:
            conn_tries += 1 # sometimes the daemon doesn't set up straight away
            if conn_tries > 5: 
                raise #but if we've waited 5 secs, let's give up
            time.sleep(1)
    _setup_fixture(_connection)
    
def _setup_daemon():
    """
    Instantiate a new mongo daemon and corresponding connection.
    """
    global _daemon
    global _connection
    
    _daemon = util.MongoDaemon(**_settings)
    return _daemon

def _setup_fixture(connection=None):
    """
    setup a test collection on a server
    set up, then index, content.
    
    TODO: purge *all* collections from db to avoid clashes
    
    Call directly if you don't want to use the one-off test server
    """
    global _collection
    global _database
    global _connection
    
    if connection is None: connection = _connection
    
    _database = connection['test']
    _database['system.js'].remove()    
    _collection = _database['items']
    _collection.remove()
    util.load_all_server_functions(_database)
    util.load_fixture('jstests/_fixture-basic.json', _collection)

def teardown_module():
    if _connection:
        _connection.disconnect()
    if _daemon:
        _daemon.destroy()

def test_simple_search():
    collection = _database['search_works']
    collection.remove()
    stdout, stderr = util.load_fixture('jstests/_fixture-basic.json', collection)
    conf = _database['search_.config']
    conf.remove()
    conf.insert({
      'collection_name' : 'search_works',
      'indexes': {
          'default_': {'fields': {'title': 5, 'content': 1}},
          'title': {'fields': {'title': 1}}
      }
    })
    
    stdout, stderr = mongo_search.ensure_text_index(collection)
    
    results = mongo_search.raw_search(collection, u'fish')
    
    assert_equals(
      list(results.find()),
      [{u'_id': 1.0, u'value': 0.72150482058559517},
       {u'_id': 3.0, u'value': 0.32510310522208458}]
    )
    
    nice_results = mongo_search.search(collection, u'fish')

    assert_equals(
      list(nice_results),
      [{u'_id': 1.0, u'value': {u'content': u'groupers like John Dory', u'_id': 1.0, u'score': 0.72150482058559517, u'title': u'fish', u'category': u'A' }},
       {u'_id': 3.0, u'value': {u'content': u'whippets kick groupers', u'_id': 3.0, u'score': 0.32510310522208458, u'title': u'dogs and fish', u'category': u'B' }}]
    )

def test_oo_search():
    collection = mongo_search.SearchableCollection(
      _database['oo_search_works']
    )
    collection.remove()
    stdout, stderr = util.load_fixture('jstests/_fixture-basic.json', collection)
    
    stdout, stderr = collection.configure_text_index_fields({'title': 5, 'content': 1})
    stdout, stderr = collection.configure_text_index_fields({'title': 1}, 'title')
    
    stdout, stderr = collection.ensure_text_index()
    

    # NO raw search for now
    # we could do this, but we would want to use a fields param like with .find()
    
    # results = collection._search(u'fish')
    # 
    # assert_equals(
    #   list(results.find()),
    #   [{u'_id': 1, u'value': 0.72150482058559517},
    #    {u'_id': 3, u'value': 0.32510310522208458}]
    # )
 
    assert_equals(list(collection.search(u'fish')), [
        {u'content': u'groupers like John Dory', u'_id': 1.0, u'score': 0.72150482058559517, u'title': u'fish', u'category': u'A' },
        {u'content': u'whippets kick groupers', u'_id': 3.0, u'score': 0.32510310522208458, u'title': u'dogs and fish', u'category': u'B' }])
        
    assert_equals(list(collection.search(u'fish', spec={u'category': u'A'})), [
        {u'content': u'groupers like John Dory', u'_id': 1.0, u'score': 0.72150482058559517, u'title': u'fish', u'category': u'A' }])
        
    assert_equals(list(collection.search(u'dog whippet', limit=1)), [
        {u'content': u'whippets kick groupers', u'_id': 3.0, u'score': 0.27585913234480763, u'title': u'dogs and fish', u'category': u'B' }])
    assert_equals(list(collection.search(u'whippets', skip=1, spec={u'category': u'B'})), [
        {u'content': u'whippets kick mongrels', u'_id': 2.0, u'score': 0.1706438640480763, u'title': u'dogs', u'category': u'B' }])
    assert_equals(list(collection.search(u'whippet', spec={u'category': u'Z'})), [])
    assert_equals(list(collection.search(u'whippet', skip=2, spec={u'category': u'Z'})), [])
    assert_equals(list(collection.search(u'spurgle', limit=10)), [])
    
    cursor = collection.search(u'dog whippet')
    assert_equals(cursor.count(), 2)
    cursor.skip(1)
    cursor.limit(1)
    assert_equals(cursor[0], 
        {u'content': u'whippets kick mongrels', u'category': u'B', u'_id': 2, u'score': 0.72398060061762026, u'title': u'dogs'})
    assert_equals(len(list(cursor)), 1)
    
    cursor = collection.search(u'dog')
    cursor.limit(10)
    assert_equals(cursor.count(), 2)
    assert_equals(list(cursor), 
        [{u'content': u'whippets kick mongrels', u'_id': 2.0, u'score': 0.8532193202403815, u'title': u'dogs', u'category': u'B' },
        {u'content': u'whippets kick groupers', u'_id': 3.0, u'score': 0.32510310522208458, u'title': u'dogs and fish', u'category': u'B' }])
 
    cursor = collection.search(u'kick', skip=1, limit=5)
    assert_equals(list(cursor), 
        [{u'content': u'whippets kick mongrels', u'_id': 2.0, u'score': 0.1706438640480763, u'title': u'dogs', u'category': u'B' }])
    assert_equals(cursor.count(), 2)
   


def test_per_field_search():
    collection = mongo_search.SearchableCollection(
      _database['oo_per_field_works']
    )
    collection.remove()
    stdout, stderr = util.load_fixture('jstests/_fixture-per_field.json', collection)
    collection.configure_text_index_fields({'title': 5, 'content': 1})
    collection.configure_text_index_fields({'title': 1}, 'title')
    collection.configure_text_index_fields({'content': 1}, 'content_idx')
         
    stdout, stderr = collection.ensure_text_index()

    assert_equals(list(collection.search(u'dog')), [
        { u'_id' : 3, u'title' : u'dogs & fish', u'content' : u'whippets kick groupers', u'category': u'B', u'score': 0.68680281974344504  },
        { u'_id' : 2, u'title' : u'dogs', u'content' : u'whippets kick mongrels and no fish are involved', u'category': u'B', u'score': 0.65447153370732369  },
        {u'_id' : 1, u'title' : u'fish', u'content' : u'groupers like John Dory are not dogs', u'category': u'A', u'score': 0.13203025163465576 }])
    
    assert_equals(list(collection.search(u'dog')), list(collection.search({u'default_': 'dog'})))
    
    assert_equals(list(collection.search({u'title': u'dog'})), [    
        { u'_id' : 2, u'title' : u'dogs', u'content' : u'whippets kick mongrels and no fish are involved', u'category': u'B', u'score': 1  },
        { u'_id' : 3, u'title' : u'dogs & fish', u'content' : u'whippets kick groupers', u'category': u'B', u'score': 0.7071067811865475  }])
        
    assert_equals(list(collection.search({u'content_idx': u'dogs'})),  [{ u'_id' : 1, u'title' : u'fish', u'content' : u'groupers like John Dory are not dogs', u'category': u'A', u'score': 0.1757748711858504 }])
    
    assert_equals(list(collection.search({u'title': u'fish'}, {'category': 'B'})), [
        { u'_id' : 3, u'title' : u'dogs & fish', u'content' : u'whippets kick groupers', u'category': u'B', u'score': 0.7071067811865475  }])

    assert_equals(list(collection.search({u'title': u'fish dog'})), [
        { u'_id' : 3, u'title' : u'dogs & fish', u'content' : u'whippets kick groupers', u'category': u'B', u'score': 0.99999999999999989 }])
    
    assert_equals(list(collection.search({u'title': u'dog'}, spec={u'category': u'Z'})), [])
    assert_equals(list(collection.search({u'content_idx': u'whippet'}, skip=2, spec={u'category': u'Z'})), [])
    assert_equals(list(collection.search({u'content_idx': u'spurgle'}, limit=10)), [])
    assert_equals(list(collection.search({u'title': u'dogs whippet'})), [])
    
    assert_raises(mongo_search.SearchIndexNotConfiguredException, collection.search, {u'not_index_name': 'dog'})
   

# def test_stemming():
#     analyze = whoosh_searching.search_engine().index.schema.analyzer('content')
#     assert list(analyze(u'finally'))[0].text == u'final' # so porter1 right now
#     assert list(analyze(u'renegotiation'))[0].text == u'renegoti' # so porter1 right now
#     assert list(analyze(u'cat'))[0].text == u'cat' # so porter1 right now
# 
# def test_stemmed_search():
#     se = whoosh_searching.search_engine()
#     results = list(se.search(u'distinguished')) # should be stemmed to distinguish
#     assert len(results) == 1
#     assert results[0]['id'] == u'24455'
#     
# def greater_than(a, b):
#     """
#     test helper assertion
#     """
#     assert a>b
# 
# def test_get_field():
#     """
#     does our dict traverser descend just how we like it?
#     """
#     #fail loudly for nonsense schemata
#     # yield assert_raises, KeyError, get_field, {}, 'nonexistent_field'
#     yield assert_equals, get_field({}, 'nonexistent_field'), None
#     #but find members if they exist
#     yield assert_equals, get_field({'a': 5}, 'a'), [5]
#     yield assert_equals, get_field({'a': [5, 6, 7]}, 'a'), [5, 6, 7]
#     yield assert_equals, get_field({'a': {'b': [5, 6, 7]}}, 'a.b'), [5, 6, 7]
#     yield assert_equals, get_field({'a': [
#       {'b': 5},
#       {'b': 1},
#       ]}, 'a.b'), [5, 1]
#     yield assert_equals, get_field({'a': [
#       {'b': [5, 6, 7]},
#       {'b': [1, 2, 3]},
#       ]}, 'a.b'), [5, 6, 7, 1, 2, 3]
#     yield assert_equals, get_field(
#       {'artist': [
#         {'name': ['brett', 'bretto', 'brettmeister']},
#         {'name': ['tim', 'timmy']},
#       ]}, 'artist.name'), ['brett', 'bretto', 'brettmeister', 'tim', 'timmy']
#     yield assert_equals, get_field(
#       {'artist': [
#         {'name': ['brett', 'bretto', 'brettmeister']},
#         {'quality': 'nameless'},
#       ]}, 'artist.name'), ['brett', 'bretto', 'brettmeister']
