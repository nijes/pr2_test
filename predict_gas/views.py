from django.shortcuts import render
from .models import Region

def index(request):
    regions = Region.objects.all()
    return render(request, 'index.html', {'regions': regions})

