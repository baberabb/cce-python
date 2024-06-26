# * All registrations for works published abroad are filtered out of the main
#   list.
#
# * But we save in a separate dataset all references to _other_ works
#   found in those works. Those _other_ works may themselves have been
#   published abroad -- we'll have to check on the next pass.

import datetime
import json
from collections import defaultdict

from tqdm import tqdm

from model import Registration

potentially_foreign = open("output/3-potentially-foreign-registrations.ndjson", "w")


class Processor(object):
    # Before this year, everything published in the US is public
    # domain.
    CUTOFF_YEAR = datetime.datetime.utcnow().year - 95

    def __init__(self):
        self.not_books_proper = open(
            "output/3-registrations-not-books-proper.ndjson", "w"
        )
        self.foreign = open("output/3-registrations-foreign.ndjson", "w")
        self.previously_published = open(
            "output/3-registrations-previously-published.ndjson", "w"
        )
        self.too_old = open("output/3-registrations-too-early.ndjson", "w")
        self.too_new = open("output/3-registrations-too-late.ndjson", "w")
        self.in_range = open("output/3-registrations-in-range.ndjson", "w")
        self.errors = open("output/3-registrations-error.ndjson", "w")
        self.foreign_xrefs = defaultdict(list)

        self.output_for_uuid = dict()

        for i in open("output/2-cross-references-in-foreign-registrations.ndjson"):
            data = json.loads(i)
            reg = Registration(**data)
            for regnum in reg.regnums:
                self.foreign_xrefs[regnum].append(reg)
        # self.cross_references_from_renewals = json.load(open(
        #    "output/1-renewal-cross-references.json"
        # ))

    def disposition(self, registration):
        if registration.is_foreign:
            # We have good evidence that this is a foreign
            # registration.
            registration.disposition = "Foreign publication."
            return self.foreign

        book_proper = False
        for regnum in registration.regnums:
            regnum = regnum.lower().strip()
            if regnum.startswith("a"):
                if not regnum.startswith("aa") or regnum.startswith("a5"):
                    book_proper = True
            if regnum in self.foreign_xrefs:
                registration.warnings.append(
                    "Possible foreign publication -- mentioned in a registration for a likely foreign publication."
                )
                registration.extra["foreign_registration"] = self.foreign_xrefs[regnum][
                    0
                ].jsonable(compact=True)
                registration.disposition = (
                    "Possible foreign publication - check manually."
                )
                return self.foreign

        if not registration.regnums:
            return self.error(registration, "No registration number.")

        if not book_proper:
            registration.disposition = "Not a book proper."
            return self.not_books_proper

        reg_date = registration.best_guess_registration_date
        if not reg_date:
            return self.error(registration, "No registration or publication date.")
        if reg_date.year < self.CUTOFF_YEAR:
            registration.disposition = "Published before cutoff year."
            return self.too_old
        elif reg_date.year > 1963:
            registration.disposition = "Published after cutoff year."
            return self.too_new

        if registration.previously_published:
            registration.disposition = (
                "Has previous publications, which must be checked manually."
            )
            return self.previously_published

        return self.in_range

    def process(self, data):
        registration = Registration.from_json(data)
        output = self.disposition(registration)
        if registration.uuid:
            self.output_for_uuid[registration.uuid] = output

        if registration.parent:
            # In the previous step, children were processed
            # immediately after their parents. That means they're
            # processed after their parents here.

            parent_output = self.output_for_uuid[registration.parent["uuid"]]
            # In general, children are totally independent
            # registrations. However, if the 'parent' registration
            # (the one for which the most data is available) was
            # deemed to be out of range, there's a good chance the
            # 'children' are also out of range.
            if (
                parent_output
                and output == self.in_range
                and parent_output != self.in_range
            ):
                registration.disposition = "Classified with parent."
                registration.warnings.append(
                    "This registration seems okay, but it was associated with a registration which was a foreign publication, a previously published work, or out of range. To be safe, this registration will be put in the same category as its 'parent'; it should be checked manually."
                )
                output = parent_output

        json.dump(registration.jsonable(require_disposition=True), output)
        output.write("\n")

    def error(self, registration, error):
        registration.disposition = "Error"
        registration.error = error
        return self.errors


if __name__ == "__main__":
    processor = Processor()
    pbar = tqdm(unit_scale=True, desc="Filtering")
    for i in open("output/2-registrations-with-renewals.ndjson"):
        data = json.loads(i)
        processor.process(data)
        pbar.update(1)
