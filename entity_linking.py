#coding:utf-8
__author__ = 'wenqihe'

import urllib2
import urllib
import json
import sys
import re
from os import path
from multiprocessing import Process, Manager
from nltk.tokenize import sent_tokenize

reload(sys)
sys.setdefaultencoding('utf8')

"""Diffbot Entity Linker for a set of documents"""

lang_dict = {'en': 'english', 'es': 'spanish', 'zh': 'chinese'}


def run(doc_list, process_number, offset, output_dir):
    print 'Start Diffbot'
    g = open(path.join(output_dir, 'diffbot_temp%d.txt' % offset), 'w')
    index = 0
    prog = re.compile('[！|。|？|.|?|!]+')
    while index < len(doc_list):
        did = str(index + offset)
        if did in doc_list:
            doc = doc_list[did]
            if lang == 'zh':  # when it is chinese, use pattern-based sentence splitter
                sentences = prog.split(doc)
            else:
                sentences = sent_tokenize(doc, language=lang_dict[lang])
            for sent_id in xrange(0, len(sentences)):
                sent = sentences[sent_id]
                try:
                    url = 'http://nlptags.diffbot.com/%s/el?'%(lang)
                    data = {'token': '8e26a22e5b85d7360224b32b1933434d', 'text': sent}
                    data = urllib.urlencode(data)
                    print url+data
                    doc_json = urllib2.urlopen(url+data).read()
                    print doc_json
                    valid_entities = extract_annotations(doc_json)
                    for entity in valid_entities:
                        if entity['uri'] !='' and entity['surfaceForms'] !='':
                            # doc_id |surface_form | dbpedia_uri | score | ambiguityScore
                            g.write(did + '\t' + entity['surfaceForms'] + '\t' + entity['uri'] + '\t'
                                    + str(entity['score']) + '\t' + str(entity['ambiguityScore']) + '\n')
                except Exception as e:
                     print did, ':', sent_id, ': ', sent
                     print e
        index += process_number
    g.close()


def link(docFile, output_dir, process_number):
    print 'Start reading documents'
    manager = Manager()
    doc_list = manager.dict()
    f = open(docFile)
    for doc in f:
        tab = doc.find('\t')
        did = doc[:tab]
        text = doc[tab+1:]
        doc_list[did] = text
    f.close()

    print 'Start entity linking'
    processes = []
    for i in range(0, process_number):
        my_process = Process(target=run, args=(doc_list, process_number, i, output_dir))
        my_process.start()
        processes.append(my_process)
    # Wait for all threads to complete
    for t in processes:
        t.join()


    print 'Start joining the files'
    all_lines = {}
    for i in range(0, process_number):
        f = open(path.join(output_dir, 'diffbot_temp%d.txt' % i))
        for line in f:
            tab = line.find('\t')
            did = line[:tab]
            all_lines[line] = did
        f.close()

    sorted_x = sorted(all_lines.items(), key=lambda x: (int(x[1].split(':')[0]), int(x[1].split(':')[1])))
    g = open(path.join(output_dir, 'diffbot_linked.txt'), 'w')
    for tup in sorted_x:
        g.write(tup[0])
    g.close()


# Extract diffbot annotations
def extract_annotations(docJson):
    valid_entities = []
    decoded = json.loads(docJson)
    for entity in decoded['all-tags']:
        entity['surfaceForms'] = ';'.join(entity['surfaceForms'])
        entity['ambiguityScore'] = entity['scores'][0]['ambiguityScore']
        valid_entities.append(entity)
    return valid_entities


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print 'Usage: entity_linking.py -LANG -RAWTEXT_FILE -OUTPUT_DIR -PROCESS_NUMBER'
        exit(-1)
    lang = sys.argv[1]
    raw_text = sys.argv[2]  # RawText
    out_dir = sys.argv[3]  # output folder for Diffbot linking results
    process_number = int(sys.argv[4])
    link(raw_text, out_dir, process_number)  # Diffbot