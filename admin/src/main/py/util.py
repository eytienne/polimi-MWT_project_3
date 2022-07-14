import re
from typing import TextIO

# https://stackoverflow.com/questions/27945073/how-to-read-properties-file-in-python
def parse_properties(input: TextIO, separator: str = "="):
    result = {}
    for line in input:
        if re.match("^\s+#.*", line):
            continue
        if separator in line:
            # Find the name and value by splitting the string
            name, value = line.split(separator, 1)

            # Assign key value pair to dict
            # strip() removes white space from the ends of strings
            result[name.strip()] = value.strip()
    return result