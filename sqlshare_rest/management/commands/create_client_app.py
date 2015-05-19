from django.core.management.base import BaseCommand
from optparse import make_option
from oauth2_provider.models import Application
from django.contrib.auth.models import User


class Command(BaseCommand):
    help = "This creates a new client oauth app."

    option_list = BaseCommand.option_list + (
        make_option('--name',
                    dest='name',
                    help='The user-visible name of the client app'),

        make_option('--return-url',
                    dest='return_url',
                    help='The url users return to after authorizing your app'),

        make_option('--owner-username',
                    dest='username',
                    help='The login name of the user that owns this app.'),
                    )

    def handle(self, *args, **options):
        error = False
        if "return_url" not in options or not options["return_url"]:
            error = True
            print ("--return-url is required")
        if "name" not in options or not options["name"]:
            error = True
            print ("--name is required")
        if "username" not in options or not options["username"]:
            error = True
            print ("--owner-username is required")

        if error:
            return

        (user, new) = User.objects.get_or_create(username=options["username"])

        url = options["return_url"]
        code = "authorization-code"
        new_client = Application.objects.create(user=user,
                                                redirect_uris=url,
                                                client_type="confidential",
                                                authorization_grant_type=code,
                                                )

        print ("ID:%s SECRET:%s" % (new_client.client_id,
                                    new_client.client_secret))
