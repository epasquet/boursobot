import os
from pathlib import Path
# from configparser import RawConfigParser



HOME_DIR = Path(os.path.expanduser("~"))
PACKAGE_DIR = Path(HOME_DIR / "Documents" / "Dev" / "bourse")


# MAIL_CRED_FILE =  Path(PACKAGE_DIR / "mail_credentials.txt")
# config = RawConfigParser()
# config.read(MAIL_CRED_FILE)
# 
# for k in config.keys():
#    print(k)
# 
# login = config.get("configuration", "login")
# password = config.get("configuration", "password")