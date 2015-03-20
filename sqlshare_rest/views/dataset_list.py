from sqlshare_rest.views import RESTView
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from oauth2_provider.decorators import protected_resource


# Probably the approach to use?
@csrf_exempt
@protected_resource()
def dataset_list(request):
    return HttpResponse("[]")


# Probably going away?
class DatasetListView(RESTView):
    @csrf_exempt
    def GET(self, request):
        return HttpResponse("[]")
