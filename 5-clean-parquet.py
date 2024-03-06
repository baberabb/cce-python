import json

import pandas as pd
import polars as pl
from dateutil import parser
from tqdm import tqdm


def is_valid_year(x):
    try:
        parsed_date = parser.parse(x, yearfirst=True)
        if 1929 < parsed_date.year < 1964:
            return True
        else:
            return False
    except:
        return True


def is_valid_regnum(x: dict) -> bool:
    FOREIGN_PREFIXES = {"AF", "AFO", "AF0"}
    regnum = x["regnum"]
    if isinstance(regnum, str):
        if any(
            regnum.lower().startswith(prefix.lower()) for prefix in FOREIGN_PREFIXES
        ):
            return False
        elif not regnum.lower().startswith("a"):
            return False
    elif isinstance(regnum, str):
        for x in regnum:
            if any(x.lower().startswith(prefix.lower()) for prefix in FOREIGN_PREFIXES):
                return False
            if not x.lower().startswith("a"):
                return False
    return True


if __name__ == "__main__":
    # All registrations
    file_name = "/Users/baber/PycharmProjects/cce-python/output/2-registrations-with-renewals.ndjson"

    res = []
    look = []
    with open(file_name) as f:
        for line in tqdm(f):
            line = json.loads(line)
            if regnums := line.get("regnums"):
                for r in regnums:
                    if r == "A344660":
                        look.append(line)
            if "renewals" in line:
                temp = [x.get("uuid") for x in line["renewals"]]
                line["renewals"] = (
                    temp if not all(not element for element in temp) else None
                )
            else:
                line["renewals"] = None
            if "reg_dates" in line:
                temp_dates = []
                for x in line["reg_dates"]:
                    if date := x.get("_normalized"):
                        temp_dates.append(date)
                    elif date := x.get("date"):
                        temp_dates.append(date)
                    elif date := x.get("_text"):
                        temp_dates.append(date)
                line["reg_dates"] = temp_dates
                line["reg_dates"] = [x for x in line["reg_dates"] if x]
                if not line["reg_dates"]:
                    line["reg_dates"] = None
            else:
                line["reg_dates"] = None
            if "publishers" in line:
                temp = [x.get("claimants", []) for x in line["publishers"]]
                temp = [item for sublist in temp for item in sublist]
                line["publishers"] = temp if temp else None
            else:
                line["publishers"] = None
            if "warnings" in line:
                line["warnings"] = ",".join(line["warnings"])
            else:
                line["warnings"] = ""
            line.pop("extra", None)
            children = line.get("children")
            if children:
                child_ = []
                child_regnums = []
                for child in children:
                    child_regnums.extend(child.get("regnums", []))
                    if uuid := child.get("uuid"):
                        child_.append(uuid)
                if child_regnums:
                    line["child_regnums"] = [x for x in child_regnums if x]
                else:
                    line["child_regnums"] = None
                line["children"] = child_
            parent = line.get("parent", None)
            if parent:
                line["parent"] = parent.get("uuid")
                if p_regnum := parent.get("reg_num"):
                    line["parent_regnum"] = p_regnum
            else:
                parent = None
            if line.get("new_matter_claimed"):
                line["new_matter_claimed"] = ",".join(line["new_matter_claimed"])
            else:
                line["new_matter_claimed"] = None
            if dis := line.get("disposition"):
                if isinstance(dis, list):
                    line["disposition"] = ",".join(dis)
            if notes := line.get("notes"):
                if isinstance(notes, list):
                    notes = ",".join(notes)
                line["notes"] = notes
            else:
                line["notes"] = None
            for x, y in line.items():
                if not y:
                    line[x] = None
            res.append(line)

    df = pl.from_dicts(res, infer_schema_length=None)
    df.write_parquet("registrations_with_ren.parquet")

    # Registations matched/unmatched
    file_list = {
        "registrations_not_renewed": "output/FINAL-not-renewed.ndjson",
        "registrations_all": "/Users/baber/PycharmProjects/cce-python/output/0-parsed-registrations.ndjson",
    }
    res = []
    for split, file_name in file_list.items():
        with open(file_name) as f:
            for line in tqdm(f):
                line = json.loads(line)
                if "renewals" in line:
                    temp = [x.get("uuid", "") for x in line["renewals"]]
                    line["renewals"] = (
                        temp if not all(not element for element in temp) else None
                    )
                else:
                    line["renewals"] = None
                if "reg_dates" in line:
                    line["reg_dates"] = [
                        x.get("_normalized", "") for x in line["reg_dates"]
                    ]
                else:
                    line["reg_dates"] = []
                if "publishers" in line:
                    temp = [x.get("claimants", []) for x in line["publishers"]]
                    temp = [item for sublist in temp for item in sublist]
                    line["publishers"] = temp if temp else None
                else:
                    line["publishers"] = None
                if "warnings" in line:
                    line["warnings"] = ",".join(line["warnings"])
                else:
                    line["warnings"] = ""
                line.pop("extra", None)
                if "group_title" not in line:
                    line["group_title"] = ""
                if "group_uuid" not in line:
                    line["group_uuid"] = ""
                children = line.get("children", None)
                if children:
                    child_ = {"regnums": [], "reg_dates": [], "publishers": []}
                    for child in children:
                        regnum = child.get("regnums")
                        if regnum:
                            child_["regnums"].extend(regnum)
                        regdate = child.get("reg_dates", [])
                        if regdate:
                            regdate = regdate[0].get("date", "")
                            if regdate:
                                child_["reg_dates"].extend([regdate])
                        publisher_ = child.get("publishers")
                        if publisher_:
                            child_["publishers"].extend(child_.get("claimants", ""))
                    line["children"] = child_
                parent = line.get("parent", None)
                if parent:
                    if not line.get("title"):
                        line["title"] = parent.get("title")
                    if not line.get("authors"):
                        line["authors"] = parent.get("authors")
                    line["reg_dates"].extend(
                        parent.get("reg_dates", [{}])[0].get("date", [])
                    )
                    if claimant := parent.get("publishers"):
                        claimant = claimant[0].get("claimants")
                        if claimant:
                            if not isinstance(claimant, list):
                                claimant = [claimant]
                            if line.get("publishers"):
                                line["publishers"].extend(claimant)
                            else:
                                line["publishers"] = claimant
                    line["parent"] = parent.get("uuid")
                    line["regnums"].extend(parent.get("regnums", []))
                else:
                    parent = None
                line.pop("previous_regnums", None)
                if line.get("regnums"):
                    assert isinstance(line["regnums"], list)
                else:
                    line["regnums"] = None
                line.pop("warnings", None)
                line.pop("renewals", None)
                line.pop("children", None)
                if line.get("disposition"):
                    if isinstance(line["disposition"], list):
                        line["disposition"] = line["disposition"][0]
                if not parent or not parent.get("renewals"):
                    res.append(line)
        df = pd.DataFrame(res)
        df.to_parquet(f"output/{split}.parquet", index=False)

    # Renewals
    (
        pl.scan_ndjson(
            "/Users/baber/PycharmProjects/cce-python/output/2-renewals-with-no-registrations.ndjson"
        )
    ).collect().to_pandas().to_parquet("output/renewals-no-regs.parquet")

    (
        (
            pl.scan_ndjson(
                "/Users/baber/PycharmProjects/cce-python/output/2-renewals-with-registrations.ndjson"
            )
        ).collect()
    ).to_pandas().to_parquet("output/renewals-with-regs.parquet")
