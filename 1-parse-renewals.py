# This script converts each copyright renewal record from CSV to
# a JSON format similar to (but much simpler than) that created by
# 0-parse-registrations.py.
import json
import os
from collections import defaultdict
from csv import DictReader

from tqdm import tqdm

from model import Renewal


class Parser(object):

    def __init__(self):
        self.pbar = tqdm(unit_scale=True, desc='Parsing Renewals')
        self.cross_references = defaultdict(list)

    def process_directory_tree(self, path):
        for i in os.listdir(path):
            if not i.endswith('tsv'):
                continue
            if i == 'TOC.tsv':
                continue
            for entry in self.process_file(os.path.join(path, i)):
                yield entry
                self.pbar.update(1)

    def process_file(self, path):
        with open(path, 'rt') as f:
            for line in DictReader(f, dialect='excel-tab'):
                yield Renewal.from_dict(line)


if __name__ == '__main__':
    cross_ref = defaultdict(list)
    with open("llm/renewals-from-lm.ndjson") as f:
        for line in f:
            res = json.loads(line)
            if uuid:= res.get("uuid"):
                cross_ref[uuid].append(res)
    with open("output/1-parsed-renewals.ndjson", "w") as output:
        parser = Parser()
        for parsed in parser.process_directory_tree("renewals/data"):
            # if parsed.regnum:
            #     if "A52449" in parsed.regnum:
            #         print("hello")
            if parsed.uuid in cross_ref:
                c = cross_ref[parsed.uuid][0]
                c_auth: list = c.get("author")
                c_regnum: list = c.get("regnum")
                c_renewal_id: list = c.get("renewal_id", [])
                c_title: str = c.get("title")
                c_claim: list = c.get("claimants")

                if not parsed.renewal_id:
                    if c_renewal_id:
                        parsed.renewal_id = c_renewal_id
                if c_auth:
                    c_auth = filter(None, c_auth)
                    if c_auth:
                        parsed.author = " & ".join(c_auth)
                if c_regnum:
                    parsed.regnum.extend(c_regnum)
                    parsed.regnum = list(set(parsed.regnum))
                    if parsed.renewal_id:
                        parsed.regnum = [x for x in parsed.regnum if x != parsed.renewal_id]
                if c_title:
                    parsed.title = c_title
                if c_claim and None not in c_claim:
                    if parsed.claimants:
                        try:
                            parsed.claimants += " |".join(c_claim)
                        except:
                            pass
                    else:
                        parsed.claimants = c_claim
            json.dump(parsed.jsonable(), output)
            output.write("\n")
