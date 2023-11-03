# This is a placeholder lambda for the terraform
# If you change the name of this file or the function name tell Joe, 
# or fix the terraform yourself!

import logging

logging.getLogger().setLevel(logging.INFO)

def handler(event, context):
    logging.info("Placeholder Lambda!")