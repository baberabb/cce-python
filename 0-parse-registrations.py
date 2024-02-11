# This script converts each copyright registration record from XML to
# JSON, with a minimum of processing.
import json
from pdb import set_trace
import os
from collections import defaultdict
from lxml import etree
from tqdm import tqdm
from model import Registration


class Parser(object):

    def __init__(self):
        self.parser = etree.XMLParser(recover=True)
        self.seen_tags = set()
        self.seen_publisher_tags = set()

    def process_directory_tree(self, path):
        pbar = tqdm(unit_scale=True)
        for dir_, subdirs, files in os.walk(path):
            if 'alto' in subdirs:
                subdirs.remove('alto')
            for i in files:
                if not i.endswith('xml'):
                    continue
                for entry in self.process_file(os.path.join(dir_, i)):
                    yield entry
                    pbar.update(1)

    def process_file(self, path):
        tree = etree.parse(path, self.parser)
        for e in tree.xpath("//copyrightEntry"):
            for registration in Registration.from_tag(e, include_extra=False):
                yield registration.jsonable()


if __name__ == '__main__':
    if not os.path.exists("output"):
        os.mkdir("output")
    with open("output/0-parsed-registrations.ndjson", "w") as output:
        for parsed in Parser().process_directory_tree("registrations/xml"):
            json.dump(parsed, output, sort_keys=True)
            output.write("\n")
