# This is a placeholder lambda for the terraform
# If you change the name of this file or the function name tell Joe,
# or fix the terraform yourself!

# In that case we will need to change (at least) the handler argument in
# lambda.tf

# I'm not sure if we can call this just "handler". It might cause problems
# with AWS

import logging

logging.getLogger().setLevel(logging.INFO)


def processing_handler(event, context):
    logging.info("Placeholder Lambda!")
