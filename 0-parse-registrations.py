# This script converts each copyright registration record from XML to
# JSON, with a minimum of processing.
import json
import os
import uuid

from lxml import etree
from tqdm import tqdm

from model import Registration


class Parser:
    def __init__(self):
        self.parser = etree.XMLParser(recover=True)
        self.seen_tags = set()
        self.seen_publisher_tags = set()

    def process_directory_tree(self, path, xpath_="//copyrightEntry"):
        pbar = tqdm(unit_scale=True, unit=xpath_[2:], desc=f"Processing {xpath_}")
        for dir_, subdirs, files in os.walk(path):
            if "alto" in subdirs:
                subdirs.remove("alto")
            for i in files:
                if not i.endswith("xml"):
                    continue
                for entry in self.process_file(os.path.join(dir_, i), xpath_=xpath_):
                    yield entry
                    pbar.update(1)


    # def process_file(self, path):
    #     tree = etree.parse(open(path), self.parser)
    #     for e in tree.xpath("//copyrightEntry"):
    #         for registration in Registration.from_tag(e, include_extra=False):
    #             yield registration.jsonable()


    def process_file(self, path, xpath_="//copyrightEntry"):
        tree = etree.parse(path, self.parser)
        year = tree.find('.//year').text
        # outside_group_xpath = f"{xpath_}[not(ancestor::entryGroup)]"
        for e in tree.xpath(xpath_):
            for registration in Registration.from_tag(e, include_extra=True):
                registration.year = year
                yield registration.jsonable()
        # for e in tree.xpath(xpath_):
        #     for registration in Registration.from_tag(e, include_extra=False):
        #         yield registration.jsonable()
        # for group in tree.xpath("//entryGroup"):
        #     group_uuid = uuid.uuid4().hex  # Generate unique UUID for each entryGroup
        #     count = 0
        #     # Iterate over entries within the current entryGroup
        #     for e in group.xpath(f"{xpath_[2:]}"):
        #         for registration in Registration.from_tag(
        #             e, include_extra=True, group_uuid=group_uuid
        #         ):
        #             registration.year = year
        #             yield registration.jsonable()
        #             count += 1
        for e in tree.xpath("//crossRef"):
            for registration in Registration.from_crossref_tag(e):
                registration.year = year
                res = registration.jsonable()
                res["crossRef"] = "True"
                yield res


if __name__ == "__main__":
    if not os.path.exists("output"):
        os.mkdir("output")
    with open("output/0-parsed-registrations.ndjson", "w") as output, open(
        "output/0-parsed-registrations-crossRef.ndjson", "w"
    ) as cross:
        for parsed in Parser().process_directory_tree("registrations/xml"):
            if parsed.get("crossRef"):
                json.dump(parsed, cross, sort_keys=True)
                cross.write("\n")
            else:
                json.dump(parsed, output, sort_keys=True)
                output.write("\n")
    # with open("output/0-parsed-registrations-cross-ref.ndjson", "w") as output:
    #     for parsed in Parser().process_directory_tree("registrations/xml", xpath_="//crossRef"):
    #         json.dump(parsed, output, sort_keys=True)
    #         output.write("\n")
