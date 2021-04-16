import logging as logme
from asyncio import get_event_loop, new_event_loop, set_event_loop

from twint.run import Twint
import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

class CustomTwint(Twint):
    def __init__(self, config):
        super().__init__(config)
        self.token._session.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux ppc64le; rv:75.0) Gecko/20100101 Firefox/75.0'})
        logger.info(f"Current headers: {str(self.token._session.headers)}")


def custom_run(config, callback=None):
    logme.debug(__name__ + ':run')
    try:
        get_event_loop()
    except RuntimeError as e:
        if "no current event loop" in str(e):
            set_event_loop(new_event_loop())
        else:
            logme.exception(__name__ + ':run:Unexpected exception while handling an expected RuntimeError.')
            raise
    except Exception as e:
        logme.exception(
            __name__ + ':run:Unexpected exception occurred while attempting to get or create a new event loop.')
        raise

    get_event_loop().run_until_complete(CustomTwint(config).main(callback))
