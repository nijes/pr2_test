from django.shortcuts import render
from django.http import JsonResponse
from .models import AvgTemp, CitygasCost, Demand, Heatindex, Household, Importindex, Region, Relativeprice, Supply, Lngimport
from django.db.models import Count, Avg, Sum
from django.core.cache import cache

region_dic = {1: '서울', 2: '부산', 3:'대구', 4: '인천', 5: '광주',
               6: '대전', 7: '울산', 8: '경기', 9: '강원',
               10: '충북', 11: '충남', 12: '전북', 13: '전남',
               14: '경북', 15: '경남', 16: '제주', 17: '세종'}

def demand(request):
    if request.method == 'GET':
        req_dic = {}
        req_dic['graph1'] = demand1()
        req_dic['graph2'] = demand2()
        req_dic['graph3'] = demand3()
        req_dic['graph4'] = demand4()
        return render(request, 'demand.html', {'data': req_dic})
    else:
        regionid = int(request.POST['regionid'])
        regionname = region_dic[regionid]
        start_year = int(request.POST['startyear'])
        end_year = int(request.POST['endyear'])
        ajax_dic = {}
        ajax_dic['graph1'] = demand1(start_year, end_year, regionid)
        ajax_dic['graph2'] = demand2(start_year, end_year)
        #ajax_dic['graph3'] = demand3(regionid)
        ajax_dic['graph4'] = demand4(regionid)
        ajax_dic['regionid'] = regionid
        ajax_dic['regionname'] = regionname
        ajax_dic['startyear'] = start_year
        ajax_dic['endyear'] = end_year
        return JsonResponse(ajax_dic)


def demand1(start_year=2001, end_year=2020, regionid=1):
    dic = {}
    for year in range(start_year, end_year + 1):
        demands = Demand.objects.filter(year=year, regionid=regionid)
        # mysql에 지역별로 데이터가 순서대로 들어가있지않기 때문에 정렬 Logic생성
        month_data_list = []
        for demand in demands:
            month_data_list.append(str(demand.month).zfill(2) + str(demand.demand))
        month_data_list.sort(key=lambda x: int(x[:2]))
        # print(month_data_list)
        dic[str(year)] = (list(map(lambda x: float(x[2:]), month_data_list)))
    return dic



def demand2(start_year=2001, end_year=2020):
    dic = {}
    for regionid in range(1, 18):
        sum_value = sum(Demand.objects.filter(regionid=regionid, year__gte=start_year, year__lte=end_year).values_list('demand', flat=True))
        dic[region_dic[regionid]] = sum_value
    all_value = sum(dic.values())
    for region, value in dic.items():
        dic[region] = value/all_value * 100
    return dic



def demand3():
    dic = cache.get('demand3', None)
    if not dic:
        dic = {}
        dic['supply'] = list(map(lambda x: int(x), Supply.objects.values('year').annotate(Sum('supply')).values_list('supply__sum', flat=True).order_by('year')))
        dic['demand'] = list(map(lambda x: int(x), Demand.objects.values('year').filter(year__gte=2001).annotate(Sum('demand')).values_list('demand__sum', flat=True).order_by('year')))
        dic['diff'] = list(map(lambda x,y:x-y, dic['supply'], dic['demand']))
        cache.set('demand3', dic, 86400)
    return dic



def demand4(regionid=1):
    dic = cache.get(f'demand4_{regionid}', None)
    if not dic:
        demand_list = list(Demand.objects.filter(regionid=regionid).values('month').annotate(Avg('demand')).order_by('month'))
        dic = {}
        dic['demand'] = demand_list
        cache.set(f'demand4_{regionid}', dic, 86400)
    return dic