/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var dom = document.getElementById("echart-container");
var myChart = echarts.init(dom);
function createNumCurveEndPoint() {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        var newBaseURI = words.slice(0, ind + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/executorNumCurve"
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        var attemptId = words[ind + 2];
        var newBaseURI = words.slice(0, ind).join('/');
        if (isNaN(attemptId)) {
            return newBaseURI + "/api/v1/applications/" + appId + "/executorNumCurve";
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/executorNumCurve";
        }
    }
    //Looks like Web UI is running in standalone mode
    //Let's get application-id using REST End Point
    $.getJSON(location.origin + "/api/v1/applications", function(response, status, jqXHR) {
        if (response && response.length > 0) {
            var appId = response[0].id;
            return location.origin + "/api/v1/applications/" + appId + "/executorNumCurve";
        }
    });
}
var endPoint = createNumCurveEndPoint();
setInterval(function () {
    $.getJSON(endPoint, function (response, status, jqXHR) {
        myChart.setOption({
            series: [{
                data: response
            }]
        });
    });
}, 20000);

$.getJSON(endPoint, function (response, status, jqXHR) {
    var option = {
        title: {
            text: '动态调度监控'
        },
        tooltip: {
            trigger: 'axis',
            formatter: function (params) {
                return params[0].name;
            },
            axisPointer: {
                animation: false
            }
        },
        xAxis: {
            type: 'time',
            splitLine: {
                show: false
            }
        },
        yAxis: {
            type: 'value',
            boundaryGap: [0, '30%'],
            splitLine: {
                show: false
            }
        },
        series: [{
            name: '模拟数据',
            type: 'line',
            showSymbol: false,
            hoverAnimation: false,
            data: response
        }]
    };
    if (option && typeof option === "object") {
        myChart.setOption(option, true);
    }
});
