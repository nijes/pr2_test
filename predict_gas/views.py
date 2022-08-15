from django.shortcuts import render
from django.http import JsonResponse
from .models import AvgTemp, CitygasCost, Demand, Heatindex, Household, Importindex, Region, Relativeprice, Supply, Lngimport
from django.db.models import Count, Avg


region_dic = {'1': '서울', '2': '부산', '3':'대구', '4': '인천', '5': '광주',
               '6': '대전', '7': '울산', '8': '경기', '9': '강원',
               '10': '충북', '11': '충남', '12': '전북', '13': '전남',
               '14': '경북', '15': '경남', '16': '제주', '17': '세종'}


def index(request):
    return render(request, 'index.html')


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
        regionname = region_dic[str(regionid)]
        select_start_year = int(request.POST['startyear'])
        select_end_year = int(request.POST['endyear'])
        ajax_dic = {}
        ajax_dic['graph1'] = supply1(select_start_year, select_end_year, regionid)
        ajax_dic['graph2'] = supply2(select_start_year, select_end_year)
        # ajax_dic['graph3'] = supply3() : 고정
        ajax_dic['graph4'] = supply4(regionid)
        ajax_dic['regionid'] = regionid
        ajax_dic['regionname'] = regionname
        ajax_dic['startyear'] = select_start_year
        ajax_dic['endyear'] = select_end_year
        return JsonResponse(ajax_dic)
        #return render(request, 'test.html', {'data': req_dic})


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
        regionname = region_dic[str(regionid)]
        select_start_year = int(request.POST['startyear'])
        select_end_year = int(request.POST['endyear'])
        ajax_dic = {}
        ajax_dic['graph1'] = demand1(select_start_year, select_end_year, regionid)
        ajax_dic['graph2'] = demand2(select_start_year, select_end_year)
        # ajax_dic['graph3'] = supply3() : 고정
        ajax_dic['graph4'] = demand4(regionid)
        ajax_dic['regionid'] = regionid
        ajax_dic['regionname'] = regionname
        ajax_dic['startyear'] = select_start_year
        ajax_dic['endyear'] = select_end_year
        return JsonResponse(ajax_dic)



def supplyfactor(request):
    req_dic = {}
    req_dic['scatter'] = scatter()
    req_dic['percentage'] = percentage()
    req_dic['box'] = box()
    return render(request, 'supplyfactor.html', {'data': req_dic})




def supply1(select_start_year=2001, select_end_year=2020, regionid=1):
    dic = {}
    for year in range(select_start_year, select_end_year + 1):
        supplys = Supply.objects.filter(year=year, regionid=regionid)
        # mysql에 지역별로 데이터가 순서대로 들어가있지않기 때문에 정렬 Logic생성
        month_data_list = []
        for supply in supplys:
            month_data_list.append(str(supply.month).zfill(2) + str(supply.supply))
        month_data_list.sort(key=lambda x: int(x[:2]))
        # print(month_data_list)
        dic[str(year)] = (list(map(lambda x: float(x[2:]), month_data_list)))
    return dic



def supply2(select_start_year=2001, select_end_year=2020):
    dic = {}
    for regionid in range(1, 18):
        sum_value = 0
        for year in range(select_start_year, select_end_year+1):
            supplys = Supply.objects.filter(year=year, regionid=regionid)
            for supply in supplys:
                sum_value += supply.supply
        dic[region_dic[str(regionid)]] = sum_value
    #print(dic)
    all_value = 0
    for val in dic.values():
        all_value += val
    for region, value in dic.items():
        dic[region] = value/all_value * 100
    #print(dic)
    return dic



def supply3():
    dic = {}
    import_list = []
    demand_list = []
    for year in range(2012, 2022):
        lngimport = Lngimport.objects.filter(year=year)[0]
        import_list.append(lngimport.import_field*1000)
        demand_list.append(lngimport.demand*1000)
    diff_list = list(map(lambda x,y:x-y, import_list, demand_list))
    dic['import'] = import_list
    dic['demand'] = demand_list
    dic['diff'] = diff_list
    return dic



def supply4(regionid=1):
    supplys = Supply.objects.filter(regionid=regionid) \
        .values('month') \
        .annotate(Avg('supply')) \
        .order_by('month')
    supply_list = []
    for supply in supplys:
        supply_list.append(supply)
    dic = {}
    dic['supply'] = supply_list
    return dic




def demand1(select_start_year=2001, select_end_year=2020, regionid=1):
    dic = {}
    for year in range(select_start_year, select_end_year + 1):
        demands = Demand.objects.filter(year=year, regionid=regionid)
        # mysql에 지역별로 데이터가 순서대로 들어가있지않기 때문에 정렬 Logic생성
        month_data_list = []
        for demand in demands:
            month_data_list.append(str(demand.month).zfill(2) + str(demand.demand))
        month_data_list.sort(key=lambda x: int(x[:2]))
        # print(month_data_list)
        dic[str(year)] = (list(map(lambda x: float(x[2:]), month_data_list)))
    return dic



def demand2(select_start_year=2001, select_end_year=2020):
    dic = {}
    for regionid in range(1, 18):
        sum_value = 0
        for year in range(select_start_year, select_end_year+1):
            demands = Demand.objects.filter(year=year, regionid=regionid)
            for demand in demands:
                sum_value += demand.demand
        dic[region_dic[str(regionid)]] = sum_value
    #print(dic)
    all_value = 0
    for val in dic.values():
        all_value += val
    for region, value in dic.items():
        dic[region] = value/all_value * 100
    #print(dic)
    return dic



def demand3(regionid=1):
    dic = {}
    # 8년 고정
    supply_value_list = []
    demand_value_list = []
    for year in range(2001, 2021):
        supply_sum_value = 0
        demand_sum_value = 0

        try:
            supplys = Supply.objects.filter(year=year, regionid=regionid)
            demands = Demand.objects.filter(year=year, regionid=regionid)
            for idx in range(12):
                supply_sum_value += supplys[idx].supply
                demand_sum_value += demands[idx].demand
        except:
            supply_sum_value += 0
            demand_sum_value += 0
        supply_value_list.append(int(supply_sum_value))
        demand_value_list.append(int(demand_sum_value))
    diff_list = list(map(lambda x, y: x - y, demand_value_list, supply_value_list))
    dic['supply'] = supply_value_list
    dic['demand'] = demand_value_list
    dic['diff'] = diff_list
    # print(dic)
    return dic



def demand4(regionid=1):
    demands = Demand.objects.filter(regionid=regionid) \
        .values('month') \
        .annotate(Avg('demand')) \
        .order_by('month')
    demand_list = []

    for demand in demands:
        demand_list.append(demand)
    dic = {}
    dic['demand'] = demand_list
    return dic



def percentage():
    dic = {}
    for regionid in range(1, 18):
        for year in range(2001, 2021):
            supplys = Supply.objects.filter(year=year, regionid=regionid)
            sum_value = 0
            for supply in supplys:
                sum_value += supply.supply

            if dic.get(region_dic[str(regionid)]) == None:
                dic[region_dic[str(regionid)]] = [sum_value]
            else:
                dic[region_dic[str(regionid)]].append(sum_value)
    return dic


def scatter():
    dic = {}

    #공급량 2013~2020년 월별 데이터, 기본
    supply_list = []
    for year in range(2013, 2021):
        for month in range(1, 13):
            sum_value = 0
            supplys = Supply.objects.filter(year=year, month=month)
            for supply in supplys:
                sum_value += supply.supply
            supply_list.append(int(sum_value/17))

    # 수요가수와 비교하기 위한 공급량 list(2013~2021 연도별 지역별)
    supply_sub1_list = []
    for year in range(2013, 2021):
        for regionid in range(1, 18):
            sum_value = 0
            supplys = Supply.objects.filter(year=year, regionid=regionid)
            for supply in supplys:
                sum_value += supply.supply
            supply_sub1_list.append(sum_value)

    # 공급량 2011~2018년 월별 데이터, for relativeprice
    supply_sub2_list = []
    for year in range(2011, 2019):
        for month in range(1, 13):
            sum_value = 0
            supplys = Supply.objects.filter(year=year, month=month)
            for supply in supplys:
                sum_value += supply.supply
            supply_sub2_list.append(int(sum_value / 17))

    demand_list = []
    for year in range(2013, 2021):
        for month in range(1, 13):
            sum_value = 0
            demands = Demand.objects.filter(year=year, month=month)
            for demand in demands:
                sum_value += demand.demand
            demand_list.append(int(sum_value / 17))

    avgTemp_list = []
    for year in range(2013, 2021):
        for month in range(1, 13):
            sum_value = 0
            avgTemps = AvgTemp.objects.filter(year=year, month=month)
            for avgTemp in avgTemps:
                if avgTemp.avgtemp == None:
                    continue
                else:
                    sum_value += avgTemp.avgtemp
            avgTemp_list.append(int(sum_value/17))

    heatindex_list = []
    for year in range(2013, 2021):
        for month in range(1, 13):
            sum_value = 0
            heatindex = Heatindex.objects.filter(year=year, month=month)[0]
            heatindex_list.append(heatindex.heatindex)

    importPriceindex_list = []
    importAmountindex_list = []
    importCostindex_list = []
    for year in range(2013, 2021):
        for month in range(1, 13):
            sum_value = 0
            importindex = Importindex.objects.filter(year=year, month=month)[0]
            importPriceindex_list.append(importindex.importpriceindex)
            importAmountindex_list.append(importindex.importamountindex)
            importCostindex_list.append(importindex.importcostindex)

    household_list = []
    for year in range(2013, 2021):
        for regionid in range(1, 18):
            sum_value = 0
            households = Household.objects.filter(year=year, regionid=regionid)
            for household in households:
                sum_value += household.household
            household_list.append(sum_value)

    citygasCost_list = []
    for year in range(2013, 2021):
        for month in range(1, 13):
            sum_value = 0
            citygasCosts = CitygasCost.objects.filter(year=year, month=month)
            for citygasCost in citygasCosts:
                if citygasCost.citygascost == None:
                    continue
                else:
                    sum_value += citygasCost.citygascost
            citygasCost_list.append(int(sum_value/17))

    #2019~2020년에 NULL데이터가 많아서 2011~2018년으로 대체
    relativeprice_list = []
    for year in range(2011, 2019):
        for month in range(1, 13):
            sum_value = 0
            relativeprice = Relativeprice.objects.filter(year=year, month=month)[0]
            relativeprice_list.append(relativeprice.relativeprice)

    #요인 데이터에 none값이 있는 경우 해당 데이터 제외시키고 저장
    dic['demand'] = list(filter(lambda x: x[1] is not None, list(map(lambda x,y : [x,y], supply_list, demand_list))))
    dic['avgtemp'] = list(filter(lambda x: x[1] is not None, list(map(lambda x,y : [x,y], supply_list, avgTemp_list))))
    dic['heatindex'] = list(filter(lambda x: x[1] is not None, list(map(lambda x,y : [x,y], supply_list, heatindex_list))))
    dic['importpriceindex'] = list(filter(lambda x: x[1] is not None, list(map(lambda x,y: [x,y], supply_list, importPriceindex_list))))
    dic['importamountindex'] = list(filter(lambda x: x[1] is not None, list(map(lambda x, y: [x, y], supply_list, importAmountindex_list))))
    dic['importcostindex'] = list(filter(lambda x: x[1] is not None, list(map(lambda x, y: [x, y], supply_list, importCostindex_list))))
    dic['household'] = list(filter(lambda x: x[1] is not None, list(map(lambda x, y: [x, y], supply_sub1_list, household_list))))
    dic['citygascost'] = list(filter(lambda x: x[1] is not None, list(map(lambda x, y: [x, y], supply_sub1_list, citygasCost_list))))
    dic['relativeprice']= list(filter(lambda x: x[1] is not None, list(map(lambda x, y: [x, y], supply_sub2_list, relativeprice_list))))

    return dic


def box():
    dic = {}
    for regionid in range(1,18):
        supply_list = []
        for year in range(2001,2020):
            supplys = Supply.objects.filter(regionid=regionid, year=year)
            for supply in supplys:
                supply_list.append(supply.supply)
        dic[region_dic[str(regionid)]] = supply_list
    return dic


def heat():
    supply = Supply.objects\
        .values('month') \
        .annotate(Avg('supply')) \
        .order_by('month')
    avgtemp = AvgTemp.objects.values('month') \
        .annotate(Avg('avgtemp')) \
        .order_by('month')
    heatindex = Heatindex.objects.values('month') \
        .annotate(Avg('heatindex')) \
        .order_by('month')
    demand = Demand.objects \
        .values('month') \
        .annotate(Avg('demand')) \
        .order_by('month')

    context = {}
    context['supply'] = supply_lst
    context['avgtemp'] = avgtemp_lst
    context['heatindex'] = heatindex_lst

    result = []
    for i in range(12):
        result.append({'month': context['supply'][i]['month'], 'supply__avg': context['supply'][i]['supply__avg'],
                       'avgtemp__avg': context['avgtemp'][i]['avgtemp__avg'],
                       'heatindex__avg': context['heatindex'][i]['heatindex__avg']})

    context['result'] = result

    return context


def household():
    household = Household.objects.values('year')\
            .annotate(Avg('household'))\
            .order_by('year')
    demand = Demand.objects.values('year')\
            .annotate(Avg('demand'))\
            .order_by('year')

    household_avg = []
    year = []
    demand_avg = []

    for col in household:
        household_avg.append(col['household__avg'])
        year.append(col['year'])
    for col in demand:
        if col['year'] >= 2001:
            demand_avg.append(col['demand__avg'])

    context = {}
    context['household_avg'] = household_avg
    context['year'] = year
    context['demand_avg'] = demand_avg

    return context


def demandfactor(request):
    #################### 난방지수-평균기온 ####################
    supply1 = Supply.objects\
        .values('month') \
        .annotate(Avg('supply')) \
        .order_by('month')
    avgtemp1 = AvgTemp.objects.values('month') \
        .annotate(Avg('avgtemp')) \
        .order_by('month')
    heatindex1 = Heatindex.objects.values('month') \
        .annotate(Avg('heatindex')) \
        .order_by('month')
    demand1 = Demand.objects\
        .values('month') \
        .annotate(Avg('demand')) \
        .order_by('month')

    context1 = {}
    context1['supply'] = supply1
    context1['avgtemp'] = avgtemp1
    context1['heatindex'] = heatindex1
    result1 = []
    for i in range(12):
        result1.append({'month': context1['supply'][i]['month'], 'supply__avg': context1['supply'][i]['supply__avg'],
                       'avgtemp__avg': context1['avgtemp'][i]['avgtemp__avg'],
                       'heatindex__avg': context1['heatindex'][i]['heatindex__avg']})


    #################### 수요가수 ####################

    household2 = Household.objects.values('year') \
        .annotate(Avg('household')) \
        .order_by('year')
    demand2 = Demand.objects.values('year') \
        .annotate(Avg('demand')) \
        .order_by('year')

    household_avg2 = []
    year2 = []
    demand_avg2 = []

    for col in household2:
        household_avg2.append(col['household__avg'])
        year2.append(col['year'])
    for col in demand2:
        if col['year'] >= 2001:
            demand_avg2.append(col['demand__avg'])


    #################### 상대가격 ####################
    relative3 = Relativeprice.objects.values('year') \
        .annotate(Avg('gasproducerprice'), Avg('oilproducerprice'), Avg('relativeprice')) \
        .order_by('year')

    gasprice3 = []
    oilprice3 = []
    relativeprice3 = []
    year3 = []
    for col in relative3:
        if col['gasproducerprice__avg'] == None:
            gasprice3.append(0)
        else:
            gasprice3.append(col['gasproducerprice__avg'])
        if col['oilproducerprice__avg'] == None:
            oilprice3.append(0)
        else:
            oilprice3.append(col['oilproducerprice__avg'])
        if col['relativeprice__avg'] == None:
            relativeprice3.append(0)
        else:
            relativeprice3.append(col['relativeprice__avg'])
        year3.append(col['year'])


    #################### 3종 지수 ####################
    importindex = Importindex.objects.values('year') \
        .annotate(Avg('importpriceindex'), Avg('importamountindex'), Avg('importcostindex')) \
        .order_by('year')

    importprice_list = []
    importamount_list = []
    importcost_list = []
    for col in importindex:
        if col['importpriceindex__avg'] == None:
            importprice_list.append(0)
        else:
            importprice_list.append(col['importpriceindex__avg'])
        if col['importamountindex__avg'] == None:
            importamount_list.append(0)
        else:
            importamount_list.append(col['importamountindex__avg'])
        if col['importcostindex__avg'] == None:
            importcost_list.append(0)
        else:
            importcost_list.append(col['importcostindex__avg'])

    #################### 도시가스요금 ####################

    citygascost = CitygasCost.objects.values('year', 'regionid') \
        .annotate(Avg('citygascost')) \
        .order_by('year')
    seoul = []
    pusan = []
    daegu = []
    incheon = []
    gwangju = []
    daejun = []
    ulsan = []
    kyeonggi = []
    kangwon = []
    chungbuk = []
    chungnam = []
    jeonbuk = []
    jeonnam = []
    kyeongbuk = []
    kyeongnam = []
    jeju = []
    sejong = []

    year5 = []

    for col in citygascost:

        if col['regionid'] == 1:
            seoul.append(col['citygascost__avg'])
            year5.append(col['year'])
        elif col['regionid'] == 2:
            pusan.append(col['citygascost__avg'])
        elif col['regionid'] == 3:
            daegu.append(col['citygascost__avg'])
        elif col['regionid'] == 4:
            incheon.append(col['citygascost__avg'])
        elif col['regionid'] == 5:
            gwangju.append(col['citygascost__avg'])
        elif col['regionid'] == 6:
            daejun.append(col['citygascost__avg'])
        elif col['regionid'] == 7:
            ulsan.append(col['citygascost__avg'])
        elif col['regionid'] == 8:
            kyeonggi.append(col['citygascost__avg'])
        elif col['regionid'] == 9:
            kangwon.append(col['citygascost__avg'])
        elif col['regionid'] == 10:
            chungbuk.append(col['citygascost__avg'])
        elif col['regionid'] == 11:
            chungnam.append(col['citygascost__avg'])
        elif col['regionid'] == 12:
            jeonbuk.append(col['citygascost__avg'])
        elif col['regionid'] == 13:
            jeonnam.append(col['citygascost__avg'])
        elif col['regionid'] == 14:
            kyeongbuk.append(col['citygascost__avg'])
        elif col['regionid'] == 15:
            kyeongnam.append(col['citygascost__avg'])
        elif col['regionid'] == 16:
            jeju.append(col['citygascost__avg'])
        elif col['citygascost__avg'] == None:
            sejong.append(0)
        else:
            sejong.append(col['citygascost__avg'])



    return render(request, 'demandfactor.html',
          {'result1': result1, 'supply1': supply1, 'avgtemp1': avgtemp1, 'heatindex1': heatindex1, 'demand1': demand1,
           'household_avg2': household_avg2, 'year2': year2, 'demand_avg2': demand_avg2,
           'gasproducerprice3': gasprice3, 'oilproducerprice3': oilprice3, 'relativeprice3': relativeprice3, 'year3': year3,
           'importprice_list': importprice_list, 'importamount_list': importamount_list, 'importcost_list': importcost_list,
           'seoul': seoul, 'pusan': pusan, 'daegu': daegu, 'incheon': incheon,
           'gwangju': gwangju, 'daejun': daejun, 'ulsan': ulsan, 'kyeonggi': kyeonggi,
           'kangwon': kangwon, 'chungbuk': chungbuk, 'chungnam': chungnam,
           'jeonbuk': jeonbuk, 'jeonnam': jeonnam, 'kyeongbuk': kyeongbuk,
           'kyeongnam': kyeongnam, 'jeju': jeju, 'sejong': sejong, 'year5': year5
           })
