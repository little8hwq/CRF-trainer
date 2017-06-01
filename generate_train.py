__author__ = 'wenqihe'


import sys
from collections import defaultdict
from os import path
from nltk.tokenize.stanford import StanfordTokenizer
from multiprocessing import Process, Manager

reload(sys)
sys.setdefaultencoding('utf8')


def generate_train(doc_list, linked_file, output_dir, process_number, offset):
    with open(linked_file) as t, open(path.join(output_dir, 'segmentation_train_diffbot_temp%d.tsv' % offset), 'w') as g:
        segmenter = StanfordTokenizer(path_to_jar='lib/stanford-postagger-full-2015-12-09/stanford-postagger.jar')
        doc2entity = defaultdict(set)
        count4entity = set()
        count4mention = 0

        for line in t:
            seg = line.strip().split('\t')
            doc2entity[seg[0]].add(seg[1])
            count4entity.add(seg[-3])
        count = 0
        index = 0
        while index < len(doc_list):
            did = str(index + offset)
            if did in doc_list:
                count += 1
                if count % 2000 == 0:
                    print count
                if did in doc2entity:
                    entity = doc2entity[did]
                    tokens = segmenter.tokenize(doc_list[did])
                    first2entity = defaultdict(list)
                    # print entity
                    for e in entity:
                        e_seg = e.split()
                        first2entity[e_seg[0]].append(e_seg)
                    for key, value in first2entity.items():
                        value.sort(key=len, reverse=True)
                    index = 0
                    # print first2entity
                    while index < len(tokens):
                        flag = False
                        if tokens[index] in first2entity:
                            candidates = first2entity[tokens[index]]
                            for candidate in candidates:
                                if tokens[index:index+len(candidate)] == candidate:
                                    for j in xrange(0+len(candidate)):
                                        g.write(tokens[index+j]+'\tENT\n')
                                    index += len(candidate)
                                    flag = True
                                    count4mention += 1
                                    break
                        if not flag:
                            g.write(tokens[index]+'\tO\n')
                            index += 1
                    g.write('\n')
            index += process_number
        print offset, ', Entity count:', len(count4entity), ', Mention count:', count4mention


def run(doc_file, linked_file, output_dir, process_number):
    print 'Start reading documents'
    manager = Manager()
    doc_list = manager.dict()
    f = open(doc_file)
    for doc in f:
        tab = doc.find('\t')
        did = doc[:tab]
        text = doc[tab+1:]
        doc_list[did] = text
    f.close()

    processes = []
    for i in range(0, process_number):
        my_process = Process(target=generate_train, args=(doc_list, linked_file, output_dir, process_number, i))
        my_process.start()
        processes.append(my_process)
    # Wait for all threads to complete
    for t in processes:
        t.join()

    print 'Start joining the files'
    g = open(path.join(output_dir, 'segmentation_train_diffbot.tsv'), 'w')
    for i in range(0, process_number):
        f = open(path.join(output_dir, 'segmentation_train_diffbot_temp%d.tsv' % i))
        for line in f:
            g.write(line)
        f.close()
        g.write('\n')
    g.close()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print 'Usage: generate_train.py -RAWTEXT_FILE -LINKED_FILE -OUTPUT_DIR -PROCESS_NUMBER'
        sys.exit(-1)

    doc_file = sys.argv[1]
    linked_file = sys.argv[2]
    output_dir = sys.argv[3]
    process_number = int(sys.argv[4])

    run(doc_file, linked_file, output_dir, process_number)
