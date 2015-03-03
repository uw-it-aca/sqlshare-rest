from django.core.urlresolvers import reverse

def missing_url(name):
    try:
        url = reverse(name)
    except Exception as ex:
        print ("Ex: ", ex)
        return True

    return False
