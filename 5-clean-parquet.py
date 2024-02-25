import copy
import json
from dateutil import parser
import polars as pl
import pandas as pd
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
        if any(regnum.lower().startswith(prefix.lower()) for prefix in FOREIGN_PREFIXES):
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


if __name__ == '__main__':
    res = []
    remaining, total = 0, 0
    with open("output/2-renewals-with-no-registrations.ndjson") as f:
        for line in f:
            each_line = json.loads(line)
            total += 1
            if is_valid_year(each_line) and is_valid_regnum(each_line):
                if isinstance(each_line["regnum"], list):
                    for regnum in each_line["regnum"]:
                        each_line_copy = copy.deepcopy(each_line)
                        each_line_copy['regnum'] = regnum
                        res.append(each_line_copy)
                        remaining += 1
                else:
                    res.append(each_line)
                    remaining += 1
    print(f"{total = }")
    print(f"{remaining = }")
    df = pl.DataFrame(res)
    df.write_parquet("output/renewals-unmatched.parquet")

    file_list = {"definitely": "output/FINAL-not-renewed.ndjson",
                 "possibly": "output/FINAL-possibly-renewed.ndjson",
                 "probably": "output/FINAL-probably-renewed.ndjson", }
    res = []
    for split, file_name in file_list.items():
        with open(file_name) as f:
            for line in tqdm(f):
                line = json.loads(line)
                if "renewals" in line:
                    temp = [x.get("uuid", "") for x in line["renewals"]]
                    line["renewals"] = temp if not all(not element for element in temp) else None
                else:
                    line["renewals"] = None
                line["source"] = split
                if "reg_dates" in line:
                    line["reg_dates"] = [x.get("_normalized", "") for x in line["reg_dates"]]
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
                if "notes" in line:
                    ','.join(line["notes"])
                else:
                    line["notes"] = ""
                if not "group_title" in line:
                    line["group_title"] = ""
                if not "group_uuid" in line:
                    line["group_uuid"] = ""
                children = line.get("children", None)
                if children:
                    for child in children:
                        regnum = child.get("regnums")
                        if regnum:
                            line["regnums"].extend(regnum)
                        regdate = child.get("reg_dates", [])
                        if regdate:
                            regdate = regdate[0].get("date", "")
                        if regdate:
                            line["reg_dates"].extend([regdate])
                parent = line.get("parent", None)
                if parent:
                    if not line.get("title"):
                        line["title"] = parent.get("title")
                    if not line.get("authors"):
                        line["authors"] = parent.get("authors")
                    line["reg_dates"].extend(parent.get("reg_dates", [{}])[0].get("date", []))
                    if claimant := parent.get("publishers"):
                        claimant = claimant[0].get("claimants")
                        if line.get("publishers"):
                            line["publishers"].extend(claimant)
                        else:
                            line["publishers"] = claimant
                    line["regnums"].extend(parent.get("regnums", []))
                line.pop("parent", None)
                line.pop("children", None)
                line.pop("previous_regnums", None)
                res.append(line)
    df = pd.DataFrame(res)
    df.to_parquet("output/final_reg.parquet")



