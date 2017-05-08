from oauth2_provider.views import AuthorizationView
from oauth2_provider.models import Application
from oauth2_provider.views.application import ApplicationOwnerIsUserMixin
from braces.views import LoginRequiredMixin
from django.views.generic import CreateView, UpdateView
from django.core.urlresolvers import reverse
from django.shortcuts import render_to_response
from django import forms
import re


class SSRegistrationForm(forms.ModelForm):
    """
    Overriding the default oauth registration form, so we can keep people from
    setting values we don't want - like bad authorization grant types.
    """
    GRANT_AUTHORIZATION_CODE = 'authorization-code'
    GRANT_IMPLICIT = 'implicit'
    GRANT_CLIENT_CREDENTIALS = 'client-credentials'
    GRANT_TYPES = (
        (GRANT_AUTHORIZATION_CODE, 'Authorization code'),
        (GRANT_IMPLICIT, 'Implicit'),
        (GRANT_CLIENT_CREDENTIALS, 'Client credentials'),
    )

    # Limit the allowed grant types
    authorization_grant_type = forms.ChoiceField(choices=GRANT_TYPES)

    # Force name to be required
    name = forms.CharField(max_length=255)

    # Make it so authorization-code is only allowed for confidential clients
    def clean_authorization_grant_type(self):
        grant_type = self.cleaned_data["authorization_grant_type"]

        if grant_type == SSRegistrationForm.GRANT_AUTHORIZATION_CODE:
            client_type = self.cleaned_data["client_type"]

            if client_type != "confidential":
                msg = ("Authorization code can only be used "
                       "with confidential clients.")
                raise forms.ValidationError(msg)

        return grant_type

    class Meta:
        model = Application
        # limit the fields that can be edited...
        fields = ['name',
                  'client_type',
                  'authorization_grant_type',
                  'redirect_uris']


class SSAuthorizationView(AuthorizationView):

    def post(self, *args, **kwargs):
        value = super(SSAuthorizationView, self).post(args, **kwargs)

        location = value.get("Location", "")
        if location and re.match("oob://", location):
            new_base = reverse("oauth_access_code")
            new_url = "%s?code=" % new_base
            location = re.sub("^oob://.*\?code=", new_url, location)
            value["Location"] = location
        return value


def access_code(request):
    return render_to_response("oauth2/access_code.html",
                              {"code": request.GET["code"]})


class SSApplicationRegistration(LoginRequiredMixin, CreateView):
    """
    View used to register a new Application for the request.user

    Overriding the registration form...
    """
    form_class = SSRegistrationForm
    template_name = "oauth_apps/application_registration_form.html"

    def form_valid(self, form):
        form.instance.user = self.request.user
        return super(SSApplicationRegistration, self).form_valid(form)


class SSApplicationUpdate(ApplicationOwnerIsUserMixin, UpdateView):
    context_object_name = 'application'
    template_name = "oauth_apps/application_form.html"

    form_class = SSRegistrationForm
