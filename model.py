from __future__ import annotations
import uuid
import datetime
from typing import Iterable
from functools import reduce

from dateutil import parser as date_parser
import json
import re


class XMLParser:
    """Helper methods for running XPath queries."""

    @classmethod
    def xpath(cls, tag, path) -> list[str | None]:
        """Find all child tags matching `path` and return a list of all
        non-empty text nodes within.
        """
        results = tag.xpath(path)
        return [x.text for x in results if x.text]

    @classmethod
    def xpath1(self, tag, path) -> str | None:
        """Find a single child tag matching `path` and return
        its text node, if any.
        """
        results = tag.xpath(path)
        if not results:
            return None
        return results[0].text

    @classmethod
    def _package(cls, tag) -> dict:
        """Package a tag as a dictionary.

        The dictionary consists of the attributes of the tag, with
        the text node as a special attribute called '_text'.
        """
        attrib = dict(tag.attrib)
        attrib['_text'] = tag.text
        return attrib

    @classmethod
    def date(cls, tag, path, allow_multiple=False, warnings=None):
        results = tag.xpath(path)
        if not results:
            if allow_multiple:
                return []
            else:
                return None
        dates = []
        for date_tag in results:
            processed = cls._parse_date_tag(date_tag, warnings)
            if processed:
                dates.append(processed)
        if allow_multiple:
            return dates

        # Only one date is allowed.
        if len(dates) > 1:
            raise Exception("Found multiple dates: %r" % dates)
        return dates[0]

    @classmethod
    def _parse_date_tag(cls, date_tag, warnings) -> dict:
        """Turn a tag containing date information into a dictionary.

        :return: A dictionary with '_text' containing the raw date
                 string and '_normalized' containing the date normalized
                 in %Y-%m-%d format (if it could be parsed).
        """
        data = cls._package(date_tag)
        raw = data.get('date') or date_tag.text
        data['_text'] = raw
        return data

    @classmethod
    def _parse_date(cls, raw, warnings=None) -> datetime.datetime | None:
        parsed = None
        # Try to parse the full date, and parse just the year and
        # month if that fails. In most cases that's all we really
        # need.
        attempts = [raw]
        if len(raw) > 7 and raw[7] == '-':
            attempts.append(raw[:7])
        for attempt in attempts:
            try:
                parsed = date_parser.parse(attempt)
                if not parsed:
                    continue
                if parsed.year > 2000 and len(raw) in (6, 7):
                    # A very common date format is '19Jun58',
                    # which date_parser parses as 2059. Subtract
                    # 100 years and we're in business.
                    parsed = datetime.datetime(
                        parsed.year - 100, parsed.month, parsed.day
                    )
                if parsed.year > 1995 or parsed.year < 1900:
                    # This is most likely a totally incorrect date, or
                    # not a date at all.
                    parsed = None
                else:
                    break
            except ValueError as e:
                continue
        if not parsed and warnings is not None:
            msg = "Could not parse date %s" % raw
            warnings.append(msg)
        return parsed


class Publisher(XMLParser):
    """Represents information about the publisher(s) associated with a
    Registration, and the time and circumstances of publication.
    """

    def __init__(self, dates=None, places=None, claimants=None, nonclaimants=None, extra=None):
        self.dates = dates or []
        self.places = places or []
        self.claimants = claimants or []
        self.nonclaimants = nonclaimants or []
        self.extra = extra or {}

    def jsonable(self, compact=False) -> dict:
        data = dict(
            dates=self.dates,
            places=self.places,
            claimants=self.claimants,
            nonclaimants=self.nonclaimants,
            extra=self.extra
        )
        if compact:
            for k in list(data.keys()):
                if not data[k]:
                    del data[k]
        return data

    @classmethod
    def from_json(cls, data) -> "Publisher":
        return cls(**data)

    @classmethod
    def from_tag(cls, publisher, warnings=None) -> "Publisher":
        """Parse publisher information from a <publisher> tag."""
        extra = dict(publisher.attrib)
        pub_dates = cls.date(
            publisher, "pubDate", allow_multiple=True,
            warnings=warnings
        )
        places = cls.xpath(publisher, "pubPlace")
        claimants: list[str] = []
        nonclaimants: list[str] = []
        for publisher_name_tag in publisher.xpath("pubName"):
            name = publisher_name_tag.text
            is_claimant = publisher_name_tag.attrib.get('claimant')
            if is_claimant == 'yes':
                destination = claimants
            else:
                destination = nonclaimants
            destination.append(name)
        return cls(pub_dates, places, claimants, nonclaimants, extra)

    def __add__(self, other: "Publisher") -> "Publisher":
        return Publisher(dates=self.dates + other.dates,
                         places=self.places + other.places,
                         claimants=self.claimants + other.claimants,
                         nonclaimants=self.nonclaimants + other.nonclaimants,
                         extra=self.extra | other.extra
                         )


class Group(XMLParser):
    def __init__(self, tag):
        self.title: str = self.xpath1(tag, "title")
        self.authors: list = self.xpath(tag, "author/authorName")
        self.publishers: list = [
            Publisher.from_tag(publisher_tag, "from group")
            for publisher_tag in tag.xpath("publisher")
        ]

    def __hash__(self):
        # Convert each publisher to its JSON-serializable form, then to a tuple of items
        publishers_tuple = tuple(
            tuple(str(sorted(publisher.jsonable(compact=True).items())))
            for publisher in self.publishers
        )
        authors_tuple = tuple(self.authors)
        # Combine title, immutable form of publishers, and authors into a single tuple for hashing
        hash_tuple = (self.title, publishers_tuple, authors_tuple)
        return hash(hash_tuple)


class Places:
    """A helper class that knows about places in the real world."""

    # Big foreign publishing cities that are sometimes mentioned
    # without the context of the country.
    FOREIGN_CITIES = {'Paris', 'London', 'Berlin'}

    def __init__(self):
        self.foreign_countries = set()
        self.foreign_country_endings = set()
        for i in json.load(open("countries.json"))['countries']:
            if i not in ("United States Of America", "Georgia"):
                self.foreign_countries.add(i)
                self.foreign_country_endings.add(", " + i)
        for name in ('England', 'Scotland', 'Eng.', 'U.K.', 'UK'):
            self.foreign_countries.add(name)
            self.foreign_country_endings.add(", " + name)

    def is_foreign(self, place) -> bool:
        """Make a best guess as to whether a place name is in
        another country.
        """
        if place.endswith('.'):
            place = place[:1]
        if place in self.FOREIGN_CITIES:
            return True
        if place in self.foreign_countries:
            return True
        if ',' in place and any(x in place for x in self.FOREIGN_CITIES):
            # This will incorrectly flag "London, Ontario" but it will
            # correctly flag "London, New York", which is more common.
            return True
        if any(place.endswith(x) for x in self.foreign_country_endings):
            return True
        return False


class Registration(XMLParser):
    PLACES = Places()

    def __init__(
            self,
            uuid: str | None = None,
            regnums: list | None = None,
            reg_dates: list | None = None,
            title: str | None = None,
            authors: list | None = None,
            notes: list | None = None,
            publishers: list | None = None,
            previous_regnums: list | None = None,
            previous_publications: list | None = None,
            new_matter_claimed: list | None = None,
            extra: dict[str, list[dict]] | None = None,
            parent: "Registration" | None = None,
            children: list[Registration] | None = None,
            xrefs: list | None = None,
            _is_foreign: bool | None = None,
            warnings: list | None = None,
            error: str | None = None,
            disposition: str | None = None,
            renewals: list | None = None,
            group_title: str | None = None,
            group_hash: int | None = None
    ):
        self.uuid = uuid
        self.regnums = [x for x in (regnums or []) if x]
        self.reg_dates = reg_dates or []
        self.title = title
        self.authors = authors or []
        self.notes = notes or []
        self.new_matter_claimed = new_matter_claimed
        self.publishers = publishers or []
        self.previous_regnums = previous_regnums or []
        self.previous_publications = previous_publications or []
        self.extra = extra or {}
        self.parent = parent
        self.children = children or []
        self.xrefs = xrefs or []
        self.warnings = warnings or []
        self.error = error
        self.disposition = disposition
        self.renewals = renewals
        self.group_title = group_title
        self.group_hash = group_hash

    def jsonable(self, include_others=True, compact=False, require_disposition=False) -> dict:
        data = dict(
            uuid=self.uuid,
            regnums=self.regnums,
            reg_dates=self.reg_dates,
            title=self.title,
            authors=self.authors,
            new_matter_claimed=self.new_matter_claimed,
            notes=self.notes,
            publishers=[self._json(p, compact=compact) for p in self.publishers],
            previous_regnums=self.previous_regnums,
            previous_publications=self.previous_publications,
            extra=self.extra,
            warnings=self.warnings,
            error=self.error,
            disposition=self.disposition,
            group_title = self.group_title,
            group_hash = self.group_hash
        )
        if not self.disposition and require_disposition:
            raise Exception("Disposition not set for %r" % data)
        if self.renewals:
            data['renewals'] = [self._json(x, compact=compact) for x in self.renewals]
        if include_others and self.parent:
            parent = self._json(self.parent, include_others=False, compact=compact)
        else:
            parent = None
        data['parent'] = parent
        if include_others:
            xrefs = [
                self._json(xref, include_others=False, compact=compact)
                for xref in self.xrefs
            ]
            children = [
                self._json(child, include_others=False, compact=compact)
                for child in self.children
            ]
        else:
            children = []
            xrefs = []
        data['children'] = children
        data['xrefs'] = xrefs
        if compact:
            for k in list(data.keys()):
                if not data[k]:
                    del data[k]
        return data

    def _json(self, x, compact=False, **kwargs) -> dict:
        if isinstance(x, dict):
            if compact:
                x = dict(x)
                for k in list(x.keys()):
                    if not x[k]:
                        del x[k]
            return x
        return x.jsonable(**kwargs)

    @property
    def renewal_key(self) -> tuple[str, str]:
        def to_set(x):
            return " ".join(sorted(self._normalize_text(x).split()))

        if not self.authors:
            author = ""
        else:
            author = self.authors[0]
        key = (to_set(self.title), to_set(author))
        return key

    @classmethod
    def from_json(cls, data) -> "Registration":
        return cls(**data)

    @classmethod
    def from_tag(cls, tag, parent=None, include_extra=True) -> Iterable["Registration"]:

        """Turn a <copyrightEntry> or <additionalEntry> tag into a sequence of
        Registration objects.

        :param tag: An etree Element representing an XML tag.

        :param parent: If `tag` is an <additionalEntry> tag, the
               Registration created for the parent <copyrightEntry>
               tag. Otherwise, None.

        :param include_extra: Parse out information that's not currently
               used in to determine renewal status.

        :yield: A single Registration for an <additionalEntry> tag;
                one or more for a <copyrightEntry> tag.
        """
        group_title, group_hash = "", None
        # test_name = ["publisher", "author/authorName", "title", "note"]
        if (group := tag.getparent()) is not None and group.tag == 'entryGroup':
            group_ = Group(group)
            publishers = group_.publishers
            if publishers:
                publishers = [reduce(lambda x, y: x + y, publishers)]
            authors = group_.authors
            group_title = group_.title
            group_hash = hash(group_)

        else:
            authors = []
            publishers = []
            title = ""

        warnings: list[str] = []
        uuid = tag.attrib.get('id', None)
        regnums = tag.attrib.get('regnum', '').split()
        reg_dates = cls.date(
            tag, "regDate", allow_multiple=True, warnings=warnings,
        ) + cls.date(
            tag, 'regdate', allow_multiple=True, warnings=warnings,
        )
        title = cls.xpath1(tag, "title")
        authors += cls.xpath(tag, "author/authorName")
        notes = cls.xpath(tag, 'note')
        publishers += [
            Publisher.from_tag(publisher_tag, warnings)
            for publisher_tag in tag.xpath("publisher")
        ]
        previous_regnums = cls.xpath(tag, "prev-regNum")
        previous_publications = cls.xpath(tag, "prevPub")

        new_matter_claimed = cls.xpath(tag, "newMatterClaimed")

        # We'll parse out these items and store the data, but they're
        # not currently important to the clearance process.
        extra = {}
        if include_extra:
            for name in [
                'edition', 'noticedate', 'series',
                'vol', 'desc', 'pubDate', 'volumes',
                'claimant', 'copies', 'affDate', 'lccn', 'copyDate', 'role',
                'page', 'copyDate',
            ]:
                tags = []
                for extra_tag in tag.xpath(name):
                    tags.append(cls._package(extra_tag))
                if tags:
                    extra[name] = tags

        registration = Registration(
            uuid=uuid, regnums=regnums, reg_dates=reg_dates,
            title=title, authors=authors, notes=notes,
            publishers=publishers, previous_regnums=previous_regnums,
            previous_publications=previous_publications,
            extra=extra, parent=parent, warnings=warnings,
            new_matter_claimed=new_matter_claimed, group_title=group_title,
            group_hash=group_hash
        )

        children: list["Registration"] = []
        for child_tag in tag.xpath("additionalEntry"):
            for child_registration in cls.from_tag(tag=child_tag, parent=registration):
                registration.children.append(child_registration)

        yield registration
        for child in children:
            yield child

    FOREIGN_PREFIXES = {"AF", "AFO", "AF0"}
    INTERIM_PREFIXES = {"AI", "AIO", "AI0"}

    PREVIOUSLY_PUBLISHED_ABROAD = re.compile(r"[pd]u[bt][.,]? abroad", re.I)
    PREVIOUSLY_PUBLISHED = re.compile(r"[pd]rev[.,]? [pd]u[bt]", re.I)
    PREVIOUSLY_REGISTERED = re.compile(r"[pd]rev[.,]? reg", re.I)
    PREVIOUSLY_SOMETHING = re.compile(r"[pd]rev[.,i]", re.I)

    def _regnum_is_foreign(self, regnum) -> bool:
        if any(regnum.startswith(x) for x in self.FOREIGN_PREFIXES):
            self.warnings.append(
                "Regnum '%s' indicates a foreign registration." % regnum
            )
            return True
        if any(regnum.startswith(x) for x in self.INTERIM_PREFIXES):
            self.warnings.append(
                "Regnum '%s' indicates an interim (and foreign) registration." % regnum
            )
            return True
        return False

    @property
    def previously_published(self) -> bool:
        """See if it looks like this work was previously published -- in which
        case we'd need to manually check for earlier registrations
        which may have been renewed.
        """
        if self.previous_publications:
            return True

        if self.new_matter_claimed:
            self.warnings.append(
                "New matter claimed (%s) implies the existence of a previous publication, which must be checked manually. New matter found in this title may be out of copyright even if the previous publication was renewed." % ", ".join(
                    self.new_matter_claimed)
            )
            return True

        for note in self.notes:
            if self.PREVIOUSLY_PUBLISHED.search(note):
                self.warnings.append(
                    "Note (%r) seems to mention a previous publication, which must be checked manually." % note
                )
                return True
            if self.PREVIOUSLY_REGISTERED.search(note):
                self.warnings.append(
                    "Note (%r) seems to mention a previous registration, which must be checked manually." % note
                )
                return True
            if self.PREVIOUSLY_SOMETHING.search(note):
                self.warnings.append(
                    "Note (%r) seems to mention... something... happening previously, most likely a publication or registration. This must be checked manually." % note
                )
                return True

        return False

    @property
    def is_foreign(self) -> bool:

        """See if it's possible to determine that this registration is for a
        foreign work, based solely on the metadata.
        """
        # Maybe the registration is a foreign or interim registration.
        for regnum in self.regnums:
            if self._regnum_is_foreign(regnum):
                return True

        # Maybe there's a previous registration number that's
        # a foreign or interim registration.
        for prev_regnum in self.previous_regnums:
            if self._regnum_is_foreign(prev_regnum):
                return True

        # Maybe the 'previous publication' information or the notes
        # says that the work was previously published abroad, without
        # giving a specific registration number.
        for field, values in (
                ("Previous publication", self.previous_publications),
                ("Note", self.notes)
        ):
            for value in values:
                if self.PREVIOUSLY_PUBLISHED_ABROAD.search(value):
                    self.warnings.append("%s %r indicates work was previously published abroad." % (field, value))
                    return True
                if 'AI.' in value or 'AI-' in value:
                    self.warnings.append(
                        "%s '%s' seems to mention an interim registration." % (field, value)
                    )
                    return True

        # Maybe the book was published in a foreign place.
        for place in self.places:
            if self.PLACES.is_foreign(place):
                self.warnings.append(
                    "Publication place '%s' looks foreign." % place
                )
                return True

        # Maybe a previous publication mentions certain keywords. These
        # are not terribly reliable, so we run this test last.
        for field, values in (
                ("Previous publication", self.previous_publications),
                ("Note", self.notes)
        ):
            for value in values:
                p = value.lower()
                for keyword in [
                    'abroad', 'american ed.', 'american edition'
                ]:
                    if keyword in p:
                        self.warnings.append(
                            "%s %r mentions the keyword '%s', which indicates this _may_ have originally been a foreign publication." % (
                                field, value, keyword
                            )
                        )
                        return True
        return False

    DATE_AND_NUMBER_XREF = re.compile(r"([0-9]{,2}[A-Z][a-z]{2}[0-9]{2})[;,] ?(A[A-Z]?[0-9-]+)")

    NUMBER_AND_DATE_XREF = re.compile(r"(A[A-Z]?[0-9-]+)[;,] ?([0-9]{,2}[A-Z][a-z]{2}[0-9]{2})")
    POSSIBLE_NUMBER_XREF = re.compile(r"(A{1,2}[0-9-]{4,})")

    @property
    def places(self) -> Iterable[str]:
        """All places mentioned in the context of where this book was published."""
        for pub in self.publishers:
            for place in pub['places']:
                yield place

    csv_row_labels = 'title parent_title author parent_author regnum parent_regnum claimants place_of_publication disposition warnings'.split()

    @property
    def csv_row(self):
        publishers = [Publisher(**p) for p in self.publishers]
        pub_places = []
        claimants = []
        for pub in publishers:
            for c in pub.claimants:
                if c:
                    claimants.append(c)
            for p in pub.places:
                if p:
                    pub_places.append(p)
        pub_places = ", ".join(pub_places)
        claimants = ", ".join(claimants)
        reg_date = self.best_guess_registration_date

        if self.parent:
            parent = Registration(**self.parent)
            parent_regnums = ", ".join(parent.regnums)
            parent_title = parent.title
            parent_author = ", ".join(parent.authors)
        else:
            parent_title = None
            parent_author = None
            parent_regnums = None

        base = [
            self.title, parent_title, ", ".join(self.authors), parent_author,
            ", ".join(self.regnums), parent_regnums, claimants, pub_places, self.disposition, "\n".join(self.warnings)
        ]

        for r in (self.renewals or []):
            r = Renewal(**r)
            base += r.csv_row
        return base

    def parse_xrefs(self):
        """Look for cross-references to other registrations in the 'notes'
        field of this registration.

        :yield: A sequence of Registration objects.
        """
        if self.notes:
            for note in self.notes:
                xref = self._xref(note)
                if xref:
                    yield xref

    def _xref(self, note):
        if not note:
            return
        regnum = date = None
        for r in [self.DATE_AND_NUMBER_XREF,
                  self.NUMBER_AND_DATE_XREF]:
            m1 = r.search(note)
            if m1:
                date, regnum = m1.groups()
                date = self._parse_date(date, self.warnings)
                if date:
                    date = date.isoformat()[:10]
                    break
        else:
            m2 = self.POSSIBLE_NUMBER_XREF.search(note)
            if m2:
                [regnum] = m2.groups()
        if not regnum:
            return None
        regnum = regnum.replace("-", "")

        if date:
            reg_dates = [date]
        else:
            reg_dates = []
        return Registration(
            regnums=[regnum], reg_dates=reg_dates, notes=[note]
        )

    def _normalize_date(self, date):
        if not date:
            return None
        parsed = self._parse_date(date['_text'], self.warnings)
        if parsed:
            date['_normalized'] = parsed.isoformat()[:10]
        else:
            date['_error'] = "Could not parse date."
        return parsed

    @property
    def registration_dates(self):
        for d in self.reg_dates:
            parsed = self._normalize_date(d)
            if parsed:
                yield parsed

    @property
    def publication_dates(self):
        for p in self.publishers:
            for d in p.get('dates', []):
                parsed = self._normalize_date(d)
                if parsed:
                    yield parsed

    @property
    def best_guess_registration_date(self):
        reg = list(self.registration_dates)
        if reg:
            return min(reg)
        pub = list(self.publication_dates)
        if pub:
            return min(pub)

    NOT_ALPHA = re.compile(r"[^0-9A-Z ]", re.I)

    @classmethod
    def _normalize_text(cls, v) -> str:
        if not v:
            return ""
        return cls.NOT_ALPHA.sub("", v).lower()

    def words_match(self, t1, t2, quotient=0.75):
        if not t1 or not t2:
            return False

        norm1 = self._normalize_text(t1)
        norm2 = self._normalize_text(t2)
        if norm1 == norm2:
            return True
        w1 = norm1.split()
        w2 = norm2.split()
        intersection = set(w1).intersection(w2)
        bigger = max(len(w1), len(w2))
        if len(intersection) > (bigger * quotient):
            return True
        return False

    def author_match(self, other_author):
        if not other_author:
            return False
        for a in self.authors:
            if self.words_match(a, other_author):
                return True
        return False

    def title_match(self, other_title):
        if self.words_match(self.title, other_title):
            return True
        return False


class Renewal(object):
    csv_row_labels = 'renewal_id renewal_date renewal_registration registration_date renewal_title renewal_author'.split()

    def __init__(self, **data):
        self.data = data

    def jsonable(self):
        return self.data

    @property
    def renewal_key(self):
        def to_set(x):
            return Registration(Registration._normalize_text(x).split())

        return to_set(self.data['title']), to_set(self.data['author'])

    def __getattr__(self, k):
        return self.data[k]

    @property
    def csv_row(self):
        return [
            self.data.get('renewal_id'),
            self.data.get('renewal_date'),
            self.regnum,
            self.data.get('reg_date'),
            self.data.get('title'),
            self.data.get('author'),
        ]

    REG_NUMBER = re.compile(r"A[A-Z]?-?[0-9]+")

    @property
    def regnum(self):
        r = self.data.get('regnum', None)
        if isinstance(r, list):
            return ", ".join(r)
        return r

    @classmethod
    def extract_regnums(cls, x):
        t = x['full_text']
        return [x.replace("-", "") for x in cls.REG_NUMBER.findall(t)]

    @classmethod
    def from_dict(cls, d):
        uuid = d['entry_id']
        regnum = d['oreg']
        reg_date = d['odat']
        author = d.get('author', None)
        title = d.get('titl', None) or d.get('title', None)
        renewal_id = d['id']
        renewal_date = d.get('rdat', None)
        new_matter = d['new_matter']
        full_text = d['full_text']
        see_also_renewal = [x for x in d['see_also_ren'].split("|") if x]
        see_also_registration = [x for x in d['see_also_reg'].split("|") if x]
        if not regnum:
            regnum = cls.extract_regnums(d)
            if regnum:
                if len(regnum) == 1:
                    [regnum] = regnum
            else:
                regnum = None
        return cls(
            uuid=uuid, regnum=regnum, reg_date=reg_date,
            renewal_id=renewal_id, renewal_date=renewal_date,
            author=author, title=title, new_matter=new_matter,
            see_also_renewal=see_also_renewal,
            see_also_registration=see_also_registration,
            full_text=full_text
        )
