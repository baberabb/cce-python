import json

from tqdm import tqdm

from model import Registration


class Output:
    COMPACT = True

    def __init__(self, base):
        self.path = "output/FINAL-%s.ndjson" % base
        self.out = open(self.path, "w")
        self.count = 0

    def output(self, i):
        json.dump(i.jsonable(compact=self.COMPACT), self.out)
        self.out.write("\n")
        self.count += 1

    def tally(self, total):
        if not total:
            return "%s: %s" % (self.path, self.count)
        return "%s: %s (%.2f%%)" % (
            self.path,
            self.count,
            self.count / float(total) * 100,
        )


yes = Output("renewed")
not_books_proper = Output("not-books-proper")
probably = Output("probably-renewed")
possibly = Output("possibly-renewed")
no = Output("not-renewed")
foreign = Output("foreign")
previously_published = Output("previously-published")
error = Output("error")
too_late = Output("too-late")
too_early = Output("too-early")
probably_not = Output("probably-not-renewed")


def destination(file, disposition):
    if isinstance(disposition, list):
        disposition = disposition[0]
    if "foreign" in file:
        return foreign
    if "registrations-too-late" in file:
        return too_late
    if "registrations-too-early" in file:
        return too_early
    if "not-books-proper" in file:
        return not_books_proper
    if "error" in file:
        return error
    if "previously-published" in file:
        return previously_published
    if disposition.startswith("Probably renewed"):
        return probably
    if disposition.startswith("Probably not renewed"):
        return probably_not
    if disposition.startswith("Possibly renewed"):
        return possibly
    if disposition.startswith("Renewed"):
        return yes
    if disposition.startswith("Not renewed"):
        return no
    else:
        print(disposition)
        return no


if __name__ == "__main__":
    in_range_outputs = [yes, probably, possibly, no, probably_not]
    all_outputs = [
        foreign,
        previously_published,
        too_late,
        too_early,
        yes,
        probably,
        possibly,
        no,
        not_books_proper,
        error,
        probably_not
    ]

    for file in tqdm(
        [
            "3-registrations-in-range",
            "3-registrations-foreign",
            "3-registrations-previously-published",
            "3-registrations-too-late",
            "3-registrations-too-early",
            "3-registrations-not-books-proper",
            "3-registrations-error",
        ],
        desc="Sorting to files",
        position=0,
    ):
        path = "output/%s.ndjson"
        with open(path % file) as f:
            for i in tqdm(f,
                          position=1,
                          leave=False,
                          desc=f"Processing file {file}"):
                data = Registration.from_json(json.loads(i))
                dest = destination(file, data.disposition)
                dest.output(data)

    in_range_total = sum(x.count for x in in_range_outputs)
    grand_total = sum(x.count for x in all_outputs)

    print("Among all publications:")
    for output in all_outputs:
        print(output.tally(grand_total))
    print("Total: %s" % grand_total)
    print("")
    print("Among first US publications in renewal range:")
    for output in in_range_outputs:
        print(output.tally(in_range_total))
    print("Total: %s" % in_range_total)
