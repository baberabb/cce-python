from pdb import set_trace
from collections import defaultdict
from model import Registration, Renewal
import json

class Comparator(object):
    def __init__(self, renewals_input_path):

        self.renewals = defaultdict(list)
        self.renewals_by_title = defaultdict(list)
        self.renewals_by_key = defaultdict(list)
        # groups are not used. Doesn't seem to be a consistent grouping.
        self.group_match = defaultdict(list)

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
            renum = regnum.replace("-", "")
            if regnum in self.renewals:
                renewals.extend(self.renewals[regnum])
        if renewals:
            renewals, disposition = self.best_renewal(registration, renewals)
            registration.disposition = disposition
            if registration.group_uuid:
                self.group_match[registration.group_uuid] = renewals
        else:
            registration.disposition = "Not renewed."
            renewals = []

        if not renewals:
            # We'll count it as a decent match if we can find a
            # renewal based solely on title/author.
            #
            # n.b. right now there seem to be no such matches.
            key = registration.renewal_key
            renewals_for_key = self.renewals_by_key[key]
            if renewals_for_key:
                renewals, disposition = self.best_renewal(registration, renewals_for_key)
                registration.disposition = "Possibly renewed, based solely on title/author match."
            
        if not renewals:
            # We'll count it as a tentative match if there has _ever_ been a renewal
            # for a book with a nearly-identical title.
            title = Registration._normalize_text(registration.title) or registration.title
            renewals_for_title = self.renewals_by_title[title]
            if renewals_for_title:
                renewals, disposition = self.best_renewal(registration, renewals_for_title)
                registration.disposition = "Possibly renewed, based solely on title match."

        # if not renewals:
        #     if registration.group_uuid in self.group_match:
        #         renewals = self.group_match[registration.group_uuid]
        #         registration.disposition = "Group match"

        for renewal in renewals:
            # These renewals have been matched; they should not be
            # output in the list of unmatched renewals.
            self.used_renewals.add(renewal)
            
        return renewals

    def best_renewal(self, registration, renewals):
        # Find a renewal based on a registration date match.
        possibilities = [x.isoformat()[:10] for x in registration.registration_dates]
        for renewal in renewals:
            if renewal.reg_date in possibilities:
                # A very strong match.
                return [renewal], "Renewed. (Date match.)"

        # At this point we have multiple renewals and no date matches.
        # Try an author match.
        for renewal in renewals:
            if registration.author_match(renewal.author):
                return [renewal], "Probably renewed. (Author match.)"

        # That didn't work. Try a title match.
        for renewal in renewals:
            if registration.title_match(renewal.title):
                return [renewal], "Probably renewed. (Title match.)"

        return renewals, "Possibly renewed, but none of these renewals seem like a good match."
