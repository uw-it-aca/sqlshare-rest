
class DBInterface(object):
    def __init__(self):
        self.username = None
        self.user_connection = None


    def _not_implemented(self, message):
        raise NotImplementedError("%s not implemented in %s" % (message, self))

    def get_user(self, user):
        self._not_implemented('get_user')


