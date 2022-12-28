from django.shortcuts import render
from django.http import JsonResponse
from .models import AvgTemp, CitygasCost, Demand, Heatindex, Household, Importindex, Region, Relativeprice, Supply, Lngimport
from django.db.models import Count, Avg, Sum
from django.core.cache import cache

region_dic = {1: '서울', 2: '부산', 3:'대구', 4: '인천', 5: '광주',
               6: '대전', 7: '울산', 8: '경기', 9: '강원',
               10: '충북', 11: '충남', 12: '전북', 13: '전남',
               14: '경북', 15: '경남', 16: '제주', 17: '세종'}

def supply(request):
    if request.method == 'GET':
        req_dic = {}
        req_dic['graph1'] = supply1()
        req_dic['graph2'] = supply2()
        req_dic['graph3'] = supply3()
        req_dic['graph4'] = supply4()
        return render(request, 'supply.html', {'data': req_dic})
    else:
        regionid = int(request.POST['regionid'])
        regionname = region_dic[regionid]
        start_year = int(request.POST['startyear'])
        end_year = int(request.POST['endyear'])
        ajax_dic = {}
        ajax_dic['graph1'] = supply1(start_year, end_year, regionid)
        ajax_dic['graph2'] = supply2(start_year, end_year)
        # ajax_dic['graph3'] = supply3() : 고정
        ajax_dic['graph4'] = supply4(regionid)
        ajax_dic['regionid'] = regionid
        ajax_dic['regionname'] = regionname
        ajax_dic['startyear'] = start_year
        ajax_dic['endyear'] = end_year
        return JsonResponse(ajax_dic)



def supply1(start_year=2001, end_year=2020, regionid=1):
    dic = {}
    for year in range(start_year, end_year + 1):
        supplys = Supply.objects.filter(year=year, regionid=regionid)
        # mysql에 지역별로 데이터가 순서대로 들어가있지않기 때문에 정렬 Logic생성
        month_data_list = []
        for supply in supplys:
            month_data_list.append(str(supply.month).zfill(2) + str(supply.supply))
        month_data_list.sort(key=lambda x: int(x[:2]))
        # print(month_data_list)
        dic[str(year)] = (list(map(lambda x: float(x[2:]), month_data_list)))
    return dic



def supply2(start_year=2001, end_year=2020):
    dic = {}
    for regionid in range(1, 18):
        sum_value = sum(Supply.objects.filter(regionid=regionid, year__gte=start_year, year__lte=end_year).values_list('supply', flat=True))
        dic[region_dic[regionid]] = sum_value
    all_value = sum(dic.values())
    for region, value in dic.items():
        dic[region] = value/all_value * 100
    return dic



def supply3():
    dic = cache.get('supply3', None)
    #print('supply3 cache:', dic)
    if not dic:
        dic = {}
        dic['import'] = list(map(lambda x: 1000*x, Lngimport.objects.values_list('import_field', flat=True).order_by('year')))
        dic['demand'] = list(map(lambda x: 1000*x, Lngimport.objects.values_list('demand', flat=True).order_by('year')))
        dic['diff'] = list(map(lambda x,y:x-y, dic['import'], dic['demand']))
        cache.set('supply3', dic, 86400)
    return dic



def supply4(regionid=1):
    dic = cache.get(f'supply4_{regionid}', None)
    #print('supply4 cache:', region_dic[regionid], dic)
    if not dic:
        supply_list = list(Supply.objects.filter(regionid=regionid).values('month').annotate(Avg('supply')).order_by('month'))
        dic = {}
        dic['supply'] = supply_list
        cache.set(f'supply4_{regionid}', dic, 86400)
    return dic