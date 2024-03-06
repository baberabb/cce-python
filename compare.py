import json
from collections import defaultdict
from dateutil import parser
from model import Registration, Renewal


class Comparator(object):
    def __init__(self, renewals_input_path):
        self.renewals = defaultdict(list)
        self.renewals_by_title = defaultdict(list)
        self.renewals_by_key = defaultdict(list)
        # groups are not used. Doesn't seem to be a consistent grouping.
        self.group_match = defaultdict(list)
        self.crossrefs = defaultdict(list)

        # get registration crossrefs as well
        with open(
            "/Users/baber/PycharmProjects/cce-python/output/0-parsed-registrations-crossRef.ndjson",
            "rt",
        ) as fc:
            for i in fc:
                res = {}
                cross = json.loads(i)
                cross_uuid = cross.get("uuid")
                res["authors"] = cross.get("authors")
                res["title"] = cross.get("title")
                if cross_uuid:
                    self.crossrefs[cross_uuid].append(res)

        with open(renewals_input_path, "rt") as f:
            for i in f:
                renewal = Renewal(**json.loads(i))
                regnum = renewal.regnum
                if not regnum:
                    regnum = []
                if not isinstance(regnum, list):
                    regnum = [regnum]
                for r in regnum:
                    r = (r or "").replace("-", "")
                    self.renewals[r].append(renewal)
                title = Registration._normalize_text(renewal.title) or renewal.title
                self.renewals_by_title[title].append(renewal)
                self.renewals_by_key[renewal.renewal_key].append(renewal)
            self.used_renewals = set()

    def renewal_for(self, registration):
        """Find a renewal for this registration.

        If there's more than one, find the best one.
        """
        renewals = []
        renewal = None
        for regnum in registration.regnums:
            regnum = regnum.replace("-", "")
            if regnum in self.renewals:
                renewals.extend(self.renewals[regnum])
        if renewals:
            # this used to ignore duplicates
            renewals, disposition = zip(*self.best_renewal(registration, renewals))
            registration.disposition = disposition
            # if registration.group_uuid:
            #     self.group_match[registration.group_uuid] = renewals
        else:
            registration.disposition = "Not renewed."
            renewals = (None,)

        if all(value is None for value in renewals):
            # We'll count it as a decent match if we can find a
            # renewal based solely on title/author.
            #
            # n.b. right now there seem to be no such matches.
            key = registration.renewal_key
            renewals_for_key = self.renewals_by_key[key]
            if renewals_for_key:
                renewals, disposition = zip(
                    *self.best_renewal(registration, renewals_for_key)
                )
                registration.disposition = (
                    "Possibly renewed, based solely on title/author match."
                )

        if all(value is None for value in renewals):
            if registration.uuid in self.crossrefs:
                crosses = self.crossrefs[registration.uuid]
                for x in crosses:
                    if cross_a := x.get("authors"):
                        if (
                            cross_a not in registration.authors
                            or cross_a not in registration.title
                            or cross_a not in x.get("title")
                        ):
                            registration.authors.extend(x.get("authors"))
                    if cross_t := x.get("title"):
                        if cross_t not in registration.authors:
                            registration.title = cross_t
                    key = registration.renewal_key
                    renewals_for_key = self.renewals_by_key[key]
                    if renewals_for_key:
                        renewals, disposition = zip(
                            self.best_renewal(registration, renewals_for_key)
                        )
                        registration.disposition = (
                            "Possibly renewed, based solely on title/author match."
                        )

        # if not renewals:
        #     # We'll count it as a tentative match if there has _ever_ been a renewal
        #     # for a book with a nearly-identical title.
        #     title = Registration._normalize_text(registration.title) or registration.title
        #     if title:
        #         renewals_for_title = self.renewals_by_title[title]
        #         if renewals_for_title:
        #             renewals, disposition = self.best_renewal(registration, renewals_for_title)
        #             registration.disposition = "Possibly renewed, based solely on title match."

        # if not renewals:
        #     if registration.group_uuid in self.group_match:
        #         renewals = self.group_match[registration.group_uuid]
        #         registration.disposition = "Group match"
        if not all(value is None for value in renewals):
            for renewal in renewals:
                # if len([x for x in renewals if x]) > 1:
                #     print("hello")
                if renewal:
                    # These renewals have been matched; they should not be
                    # output in the list of unmatched renewals.
                    self.used_renewals.add(renewal)
        else:
            renewals = []

        return renewals

    def best_renewal(self, registration, renewals) -> list[tuple[Renewal | None, str]]:
        # Find a renewal based on a registration date match.
        possibilities = [x.isoformat()[:10] for x in registration.registration_dates]
        output_renewals = []
        matched_indices = []
        for i, renewal in enumerate(renewals):
            for date in renewal.reg_date:
                if date in possibilities:
                    # A very strong match.
                    output_renewals.append((renewal, "Renewed. (Date match.)"))
                    matched_indices.append(i)
                    # return [renewal], "Renewed. (Date match.)"

        # let's look at just the year
        if not len(matched_indices) == len(renewals):
            possibilities = []
            for x in registration.registration_dates:
                if x:
                    possibilities.append(str(x.year))
            if not possibilities:
                possibilities = [registration.year]
            for i, renewal in enumerate(renewals):
                if i in matched_indices:
                    pass
                else:
                    for date in renewal.reg_date:
                        try:
                            date = str(parser.parse(date).year)
                        except:
                            pass
                        if date in possibilities:
                            output_renewals.append(
                                (renewal, "Probably renewed. (Year match.)")
                            )
                            matched_indices.append(i)

        # for i, renewal in enumerate(renewals):
        #
        #     for date in renewal.reg_date:

        # At this point we have multiple renewals and no date matches.
        # Try an author match.
        for i, renewal in enumerate(renewals):
            if i in matched_indices:
                pass
            else:
                if registration.author_match(renewal.author):
                    matched_indices.append(i)
                    output_renewals.append(
                        (renewal, "Probably renewed. (Author match.)")
                    )
                    # return [renewal], "Probably renewed. (Author match.)"

        # That didn't work. Try a title match.
        for i, renewal in enumerate(renewals):
            if i in matched_indices:
                pass
            else:
                if registration.title_match(renewal.title):
                    output_renewals.append(
                        (renewal, "Probably renewed. (Title match.)")
                    )
                    matched_indices.append(i)
                    # return [renewal], "Probably renewed. (Title match.)"
        if output_renewals:
            return output_renewals
        else:
            return [(None, "Not renewed.")]
