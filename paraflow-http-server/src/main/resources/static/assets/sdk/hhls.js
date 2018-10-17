/// <reference path="jQuery-2.1.3.min.js" />
/// <reference path="json.js" />
/// <reference path="baiduTpls.js" />
/// <reference path="../../SubSys/Client/Modules/Index/Index.js" />

/*
    本地服务
*/
var hhls = {
    callBack: function (aCallback, aPara) {
        try {
            if (aCallback) {
                if (aPara) {
                    aCallback(aPara);
                }
                else {
                    aCallback(aPara);
                }
            }
        }
        catch (cer) {
            var m = cer.message;
        }
    },
    getLocalRes: function (aResPathList, aCallback) {
        try {
            var aIndex = -1;
            var aResults = [];
            var getRes = function () {
                try {
                    aIndex++;
                    if (aIndex < aResPathList.length) {
                        var aUrl = aResPathList[aIndex];
                        var aItem = { Path: aUrl, Content: "" };
                        $.ajax({
                            url: aUrl,
                            cache: false,
                            success: function (aRes) {
                                aItem.Content = aRes;
                                aResults.push(aItem);
                                getRes();
                            },
                            error: function (a, b, c) {
                                aResults.push(aItem);
                                getRes();
                            }
                        });
                    }
                    else {
                        hhls.callBack(aCallback, aResults);
                    }
                }
                catch (cer) {; }
            }
            getRes();
        }
        catch (cer) {; }
    },
    clearElement: function (aSelector) {
        try {
            var aSubs = $(aSelector + " *");
            $.each(aSubs, function (aIndex, aItem) {
                $(aItem).remove();
            });
            $(aSelector).empty();
        }
        catch (cer) {; }
    },
    removeElement: function (aSelector) {
        try {
            if ($(aSelector).length > 0) {
                hhls.clearElement(aSelector);
                $(aSelector).remove();
            }
        }
        catch (cer) {; }
    },
    fillElement: function (aSelector, aHtml) {
        try {
            hhls.clearElement(aSelector);
            $(aSelector).html(aHtml);
        }
        catch (cer) {; }
    },
    getObjJson: function (aObj) {
        var aJson = "";
        try {
            aJson = JSON.stringify(aObj);
        }
        catch (e) {
            alert(e);
        }
        return aJson;
    },
    getJsonObj: function (aJson) {
        var aObj = null;
        try {
            aObj = eval('(' + aJson + ')');
        }
        catch (cer) {; }
        return aObj;
    },
    getClone: function (aObj) {
        return hhls.getJsonObj(hhls.getObjJson(aObj));
    }
    //返回GUID    SplitString:分隔字符串(如-)
    , getGuid: function (SplitString) {
        var aId = "";
        var aSplitString = (SplitString != null) ? SplitString : "";
        try {
            var S4 = function () {
                return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
            };
            aId = (S4() + S4() + aSplitString + S4() + aSplitString + S4() + aSplitString + S4() + aSplitString + S4() + S4() + S4());
        }
        catch (cer) {; }
        return aId;
    }
    , GetTpls: function (aTpls, aCallback) {
        try {
            var aPs = [];
            var Index = 0;
            for (var p in aTpls) {
                aPs.push(aTpls[p]);
            }
            var aParaPaths = [];
            for (var i = 0; i < aPs.length; i++) {
                aParaPaths.push(aPs[i].P);
            }
            hhls.getLocalRes(aParaPaths, function (aRes) {
                for (var i = 0; i < aRes.length; i++) {
                    aPs[i].C = aRes[i].Content;
                }
                hhls.callBack(aCallback);
            });
        }
        catch (cer) {; }
    },
    GetAgreements: function (aTpls, aCallback) {
        try {
            var aPs = [];
            var Index = 0;
            for (var p in aTpls) {
                aPs.push(aTpls[p]);
            }
            var aParaPaths = [];
            for (var i = 0; i < aPs.length; i++) {
                aParaPaths.push(aPs[i].P);
            }
            hhls.getAgreementRes(aParaPaths, function (aRes) {
                for (var i = 0; i < aRes.length; i++) {
                    aPs[i].C = aRes[i].Content;
                }
                hhls.callBack(aCallback);
            });
        }
        catch (cer) {; }
    }    
    //返回URL参数
    , getUrlHashVal: function (aUrlPara) {
        var reg = new RegExp("(^|&)" + aUrlPara + "=([^&]*)(&|$)", "i");
        var r = window.location.hash.substr(1).match(reg);
        var Res = (r != null) ? unescape(r[2]) : "";
        return Res;
    } //返回URL参数
    , getUrlParam: function (aUrlPara) {
        var reg = new RegExp("(^|&)" + aUrlPara + "=([^&]*)(&|$)"); //构造一个含有目标参数的正则表达式对象 
        var r = window.location.search.substr(1).match(reg);  //匹配目标参数 
        if (r != null) return unescape(r[2]); return ""; //返回参数值 
    }
    //返回URL参数
    , getUrlParamByDefault: function (aUrlPara, aDefault) {
        var reg = new RegExp("(^|&)" + aUrlPara + "=([^&]*)(&|$)"); //构造一个含有目标参数的正则表达式对象 
        var r = window.location.search.substr(1).match(reg);  //匹配目标参数 
        var aRes = aDefault;
        if (r != null) aRes = unescape(r[2]);
        return aRes; //返回参数值 
    },
    GetOrderviews: function (aTpls, aCallback) {
        try {
            var aPs = [];
            for (var p in aTpls) {
                aPs.push(aTpls[p]);
            }
            var aParaPaths = [];
            var aSvr = [];
            var person = Index.Datas.UserInfo.F_Caption;  // wei
            var pid = Index.Datas.UserInfo.F_ID;   // 1000
            for (var i = 0; i < aPs.length; i++) {
                aPs[i].P = aPs[i].P.replace("[person]", person).replace("[pid]", pid);
                aParaPaths.push(aPs[i].P);
                aSvr.push(aPs[i].S);
            }
            hhls.getOrderviewsRes(aParaPaths, aSvr, function (aRes) {
                for (var i = 0; i < aRes.length; i++) {
                    aPs[i].C = aRes[i].Content;
                }
                hhls.callBack(aCallback);
            });
        }
        catch (cer) {; }
    },
    getOrderviewsRes: function (aResPathList, aSvrList, aCallback) {
        try {
            var aIndex = -1;
            var aResults = [];
            var getRes = function () {
                try {
                    aIndex++;
                    if (aIndex < aResPathList.length) {
                        var aUrl = aResPathList[aIndex];
                        var aSvr = aSvrList[aIndex];
                        var aItem = { Path: aUrl, Content: "", Svr: aSvr};

                        var aSvrUrl = aSvr.C.url + aUrl
                        var aService = "http://10.77.20.101:4000/service";
                        var aToken = "Bearer " + Index.Datas.Res.resToken.C;
                        var aData = {
                            chaincode: "task",
                            args: [aSvr.C.taskId, aSvr.C.provider, "2.0", aSvrUrl, "get"],
                        };
                        var aJson = JSON.stringify(aData);
                        $.ajax({
                            type: "POST",
                            url: aService,
                            data: aJson,
                            contentType: "application/json",
                            dataType: "json",
                            jsonp: 'callback',
                            timeout: 50000,
                            cache: false,
                            beforeSend: function (request) {
                                request.setRequestHeader("Authorization", aToken);
                            },
                            success: function (aRes) {
                                var aData = [];
                                if (aRes.message != "") {
                                    try{
                                        aData = JSON.parse(aRes.message);
                                    } catch (e) {
                                        aData = [];
                                    }
                                }
                                aItem.Content = aData;
                                aResults.push(aItem);
                                getRes();
                            }, error: function (aRes) {
                                aResults.push(aItem);
                                getRes();
                            }
                        }); 
                    }
                    else {
                        hhls.callBack(aCallback, aResults);
                    }
                }
                catch (cer) {; }
            }
            getRes();
        }
        catch (cer) {; }
    },
    getAgreementRes: function (aResPathList, aCallback) {
        try {
            var aIndex = -1;
            var aResults = [];
            var getRes = function () {
                try {
                    aIndex++;
                    if (aIndex < aResPathList.length) {
                        var aUrl = aResPathList[aIndex];
                        var aItem = { Path: aUrl, Content: "" };

                        var aService = "http://10.77.20.101:4000/getagreement";
                        var aToken = "Bearer " + Index.Datas.Res.resToken.C;
                        var aData = {
                            chaincode: "task",
                            //taskname: "ticket-airline",
                            taskname: aUrl,
                        };
                        var aJson = JSON.stringify(aData);
                        $.ajax({
                            type: "POST",
                            url: aService,
                            data: aJson,
                            contentType: "application/json",
                            dataType: "json",
                            jsonp: 'callback',
                            timeout: 50000,
                            cache: false,
                            beforeSend: function (request) {
                                request.setRequestHeader("Authorization", aToken);
                            },
                            success: function (aRes) {
                                aItem.Content = aRes[0];
                                aResults.push(aItem);
                                getRes();
                            }, error: function (aRes) {
                                aResults.push(aItem);
                                getRes();
                            }
                        }); 
                    }
                    else {
                        hhls.callBack(aCallback, aResults);
                    }
                }
                catch (cer) {; }
            }
            getRes();
        }
        catch (cer) {; }
    },
    getIndex: function (aArray, aFun) {
        var aIndex = -1;
        try {
            for (var i = 0; i < aArray.length; i++) {
                var aFlag = aFun(aArray[i], i);
                if (aFlag) {
                    aIndex = i;
                    break;
                }
            }
        }
        catch (cer) {; }
        return aIndex;
    },
    saveLocalObj: function (aKey, aObj) {
        try {
            window.localStorage.setItem(aKey, hhls.getObjJson(aObj));
        }
        catch (cer) {; }
    },
    loadLocalObj: function (aKey) {
        var aObj = null;
        try {
            var ajson = window.localStorage.getItem(aKey);
            aObj = hhls.getJsonObj(ajson);
        }
        catch (cer) { aObj = null; }
        return aObj;
    },
    goUrl: function (aUrl) {
        try {
            var aNewUrl = aUrl;
            aNewUrl += aUrl.indexOf("?") >= 0 ? "&" : "?";
            aNewUrl += "rndurl=" + Math.random();
            window.location.href = aNewUrl;
        }
        catch (cer) { }
    }
};

String.prototype.replaceAll = function (reallyDo, replaceWith, ignoreCase) {
    if (!RegExp.prototype.isPrototypeOf(reallyDo)) {
        return this.replace(new RegExp(reallyDo, (ignoreCase ? "gi" : "g")), replaceWith);
    } else {
        return this.replace(reallyDo, replaceWith);
    }
}

/*
private const double x_pi = 3.14159265358979324 * 3000.0 / 180.0;

/// <summary>

/// 中国正常坐标系GCJ02协议的坐标，转到 百度地图对应的 BD09 协议坐标

/// </summary>

/// <param name="lat">维度</param>

/// <param name="lng">经度</param>

public static void Convert_GCJ02_To_BD09(ref double lat,ref double lng)

{

double x = lng, y = lat;

double z =Math.Sqrt(x * x + y * y) + 0.00002 * Math.Sin(y * x_pi);

double theta = Math.Atan2(y, x) + 0.000003 * Math.Cos(x * x_pi);

lng = z * Math.Cos(theta) + 0.0065;

lat = z * Math.Sin(theta) + 0.006;

}

/// <summary>

/// 百度地图对应的 BD09 协议坐标，转到 中国正常坐标系GCJ02协议的坐标

/// </summary>

/// <param name="lat">维度</param>

/// <param name="lng">经度</param>

public static void Convert_BD09_To_GCJ02(ref double lat, ref double lng)

{

double x = lng - 0.0065, y = lat - 0.006;

double z = Math.Sqrt(x * x + y * y) - 0.00002 * Math.Sin(y * x_pi);

double theta = Math.Atan2(y, x) - 0.000003 * Math.Cos(x * x_pi);

lng = z * Math.Cos(theta);

lat = z * Math.Sin(theta);

} 
*/