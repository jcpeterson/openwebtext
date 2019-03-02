# Import dependencies
from urllib import request as req
import re
import pycurl
import os

# Define values
BASE_URL = "https://files.pushshift.io/reddit/submissions" # No trailing slash
LINK_RE_PATTERN = "<a\s.*href=[\"'](\S+)[\"'][^>]*>\S*<\/a>"
OUTPUT_DIR = "pushshift_dumps_full"

# Define functions
def main(*args, **kwargs):
    """The main entrypoint."""

    # Get links
    link_re = re.compile(LINK_RE_PATTERN)
    raw_links = link_re.findall(req.urlopen(BASE_URL).read().decode("utf-8"))
    filtered_links = [link for link in raw_links if link.startswith("./")]
    individual_links = list(set(filtered_links))

    # Download files
    curl = pycurl.Curl()
    os.makedirs(OUTPUT_DIR)
    for link in individual_links:
        filename = link[2:]
        url = BASE_URL + "/" + filename
        with open(os.path.join(OUTPUT_DIR, filename), "wb") as file:
            curl.setopt(curl.URL, url)
            curl.setopt(curl.WRITEDATA, file)
            curl.perform()
        print("Downloaded", filename)
    curl.close()

# Execute main function
if __name__ == "__main__":
    main()
