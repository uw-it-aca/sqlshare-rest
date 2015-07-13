from sqlshare_rest.models import DatasetSharingEmail
from django.utils import timezone
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from sqlshare_rest.logger import getLogger
import re


logger = getLogger(__name__)
def send_new_emails():
    unsent = DatasetSharingEmail.objects.filter(email_sent=False)
    for email in unsent:
        _send_email(email)
        email.email_sent = True
        email.date_sent = timezone.now()
        email.save()


def _send_email(email):

    url_format = getattr(settings,
                         "SQLSHARE_SHARING_URL_FORMAT",
                         "https://sqlshare.uw.edu/sqlshare/dataset/%s")

    user_from = email.dataset.owner
    owner_name = user_from.get_full_name()

    if owner_name == "":
        owner_name = user_from.username

    url = url_format % email.access_token
    dataset_name = email.dataset.name

    values = {
        'url': url,
        'dataset': dataset_name,
        'owner_name': owner_name,
    }

    text_version = render_to_string('access_email/text.html', values)
    html_version = render_to_string('access_email/html.html', values)
    subject = render_to_string('access_email/subject.html', values)
    subject = re.sub(r'[\s]*$', '', subject)

    from_email = "sqlshare-noreply@uw.edu"
    # The sharing email's email field
    to = email.email.email

    msg = "Sending sharing email to %s.  Dataset: %s" % (to, dataset_name)
    logger.info(msg)

    msg = EmailMultiAlternatives(subject, text_version, from_email, [to])
    msg.attach_alternative(html_version, "text/html")
    try:
        msg.send()
    except Exception as ex:
        logger.error("Unable to send email to %s.  Error: %s" % (to, str(ex)))
