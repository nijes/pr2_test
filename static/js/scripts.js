

window.onload = function() {

  var area_1 = document.getElementById("KR-11");
  var area_2 = document.getElementById("KR-26");
  var area_3 = document.getElementById("KR-27");
  var area_4 = document.getElementById("KR-28");
  var area_5 = document.getElementById("KR-29");
  var area_6 = document.getElementById("KR-30");
  var area_7 = document.getElementById("KR-31");
  var area_8 = document.getElementById("KR-41");
  var area_9 = document.getElementById("KR-42");
  var area_10 = document.getElementById("KR-43");
  var area_11 = document.getElementById("KR-44");
  var area_12 = document.getElementById("KR-45");
  var area_13 = document.getElementById("KR-46");
  var area_14 = document.getElementById("KR-47");
  var area_15 = document.getElementById("KR-48");
  var area_16 = document.getElementById("KR-49");
  var area_17 = document.getElementById("KR-50");


  area_1.addEventListener('click', function(){
    area_select(area_1,1, '서울');
  });
  area_1.addEventListener('mouseover', function(){
    area_mouseover(area_1);
  });
  area_1.addEventListener('mouseout', function(){
    area_mouseout(area_1);
  });

  area_2.addEventListener('click', function(){
    area_select(area_2, 2, '부산');
  });
  area_2.addEventListener('mouseover', function(){
    area_mouseover(area_1);
  });
  area_2.addEventListener('mouseout', function(){
    area_mouseout(area_2);
  });

  area_3.addEventListener('click', function(){
    area_select(area_3, 3, '대구');
  });
  area_3.addEventListener('mouseover', function(){
    area_mouseover(area_3);
  });
  area_3.addEventListener('mouseout', function(){
    area_mouseout(area_3);
  });

  area_4.addEventListener('click', function(){
    area_select(area_4, 4, '인천');
  });
  area_4.addEventListener('mouseover', function(){
    area_mouseover(area_4);
  });
  area_4.addEventListener('mouseout', function(){
    area_mouseout(area_4);
  });

  area_5.addEventListener('click', function(){
    area_select(area_5, 5, '광주');
  })
  area_5.addEventListener('mouseover', function(){
    area_mouseover(area_5);
  });
  area_5.addEventListener('mouseout', function(){
    area_mouseout(area_5);
  });

  area_6.addEventListener('click', function(){
    area_select(area_6, 6, '대전');
  });
  area_6.addEventListener('mouseover', function(){
    area_mouseover(area_6);
  });
  area_6.addEventListener('mouseout', function(){
    area_mouseout(area_6);
  });


  area_7.addEventListener('click', function(){
    area_select(area_7, 7, '울산');
  });
  area_7.addEventListener('mouseover', function(){
    area_mouseover(area_7);
  });
  area_7.addEventListener('mouseout', function(){
    area_mouseout(area_7);
  });


  area_8.addEventListener('click', function(){
    area_select(area_8, 8, '경기도');
  });
  area_8.addEventListener('mouseover', function(){
    area_mouseover(area_8);
  });
  area_8.addEventListener('mouseout', function(){
    area_mouseout(area_8);
  });


  area_9.addEventListener('click', function(){
    area_select(area_9, 9, '강원도');
  });
  area_9.addEventListener('mouseover', function(){
    area_mouseover(area_9);
  });
  area_9.addEventListener('mouseout', function(){
    area_mouseout(area_9);
  });


  area_10.addEventListener('click', function(){
    area_select(area_10, 10, '충청북도');
  });
  area_10.addEventListener('mouseover', function(){
    area_mouseover(area_10);
  });
  area_10.addEventListener('mouseout', function(){
    area_mouseout(area_10);
  });


  area_11.addEventListener('click', function(){
    area_select(area_11, 11, '충청남도');
  });
  area_11.addEventListener('mouseover', function(){
    area_mouseover(area_11);
  });
  area_11.addEventListener('mouseout', function(){
    area_mouseout(area_11);
  });


  area_12.addEventListener('click', function(){
    area_select(area_12, 12, '전라북도');
  });
  area_12.addEventListener('mouseover', function(){
    area_mouseover(area_12);
  });
  area_12.addEventListener('mouseout', function(){
    area_mouseout(area_12);
  });


  area_13.addEventListener('click', function(){
    area_select(area_13, 13, '전라남도');
  });
  area_13.addEventListener('mouseover', function(){
    area_mouseover(area_13);
  });
  area_13.addEventListener('mouseout', function(){
    area_mouseout(area_13);
  });


  area_14.addEventListener('click', function(){
    area_select(area_14, 14, '경상북도');
  });
  area_14.addEventListener('mouseover', function(){
    area_mouseover(area_14);
  });
  area_14.addEventListener('mouseout', function(){
    area_mouseout(area_14);
  });


  area_15.addEventListener('click', function(){
    area_select(area_15, 15, '경상남도');
  });
  area_15.addEventListener('mouseover', function(){
    area_mouseover(area_15);
  });
  area_15.addEventListener('mouseout', function(){
    area_mouseout(area_15);
  });


  area_16.addEventListener('click', function(){
    area_select(area_16, 16, '제주도');
  });
  area_16.addEventListener('mouseover', function(){
    area_mouseover(area_16);
  });
  area_16.addEventListener('mouseout', function(){
    area_mouseout(area_16);
  });


  area_17.addEventListener('click', function(){
    area_select(area_17, 17, '세종');
  });
  area_17.addEventListener('mouseover', function(){
    area_mouseover(area_17);
  });
  area_17.addEventListener('mouseout', function(){
    area_mouseout(area_17);
  });

  area_select(area_1,1, '서울');

}




function area_mouseover(area){
  area.style.fill = '#DDA152';
}

function area_mouseout(area){
  const area_class = area.classList;
  if (area_class.contains('select_region')){
    area.style.fill = '#BC6C25';
  } else {
    area.style.fill = '#CCCCCC';
  }
}



function area_select(area, regionid, regionname){
  document.getElementsByClassName('select_region')[0].style.fill = '#CCCCCC';
  document.getElementsByClassName('select_region')[0].classList.remove('select_region');
  area.style.fill = '#BC6C25';
  area.classList.add('select_region');
  document.getElementById('button_region').textContent = regionname
  document.getElementById('select_region').textContent = regionid
}



// 장고 공식홈페이지에 나온 js로 ajax post 요청시 csft token 만들어 삽입하는 코드
function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

var csrftoken = getCookie('csrftoken');

//