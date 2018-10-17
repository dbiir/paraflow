/// <reference path="../plugins/jQuery/jQuery-2.1.3.min.js" />
/// <reference path="json.js" />
/// <reference path="baiduTpls.js" />
/// <reference path="hhls.js" />
/// <reference path="hhls_wxConfirm.js" />
/// <reference path="../../SubSys/Client/Modules/Index/Index.js" />


var Ac = {
    Info: {
        SvcUrl: "",
        Ak: "",
        Sk: "",
        AppCode: "",
        DBKey: "",
        WxSrc: ""
    },
    acHttpGet: function (aSvr, aUrl, aPs, aCallback) {
        try {
            var aParam = JSON.stringify(aPs);
            if (aParam.length > 2) {
                aParam = aParam.replace('{', '?');
                aParam = aParam.replace('}', '');
                aParam = aParam.replace(/"/g, '');
                aParam = aParam.replace(/:/g, '=');
                aParam = aParam.replace(/,/g, '&');
                aUrl += aParam;
            }
            //aUrl = encodeURIComponent(aUrl);
            var aSvrUrl = aSvr.C.url + aUrl;
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
                beforeSend: function (request) {
                    request.setRequestHeader("Authorization", aToken);
                },
                success: function (aCallbackData) {
                    var aData = [];
                    try {
                        if (aCallbackData.message != "") {
                            aData = JSON.parse(aCallbackData.message);
                            var aResult = { State: 1, Datas: aData };
                            hhls.callBack(aCallback, aResult);
                        } else {
                            // =""
                            var aResult = { State: 1, Datas: aData };
                            hhls.callBack(aCallback, aResult);
                        }
                    } catch (e) {
                        aData = aCallbackData[0];
                        try {
                            if (aData.requester.length > 0) {
                                aSvr.C = aData;
                                Index.doSaveAgreement(Index.Datas.Agreement);
                                Ac.acHttpGet(aSvr, aUrl, {}, function (aResult) {
                                    hhls.callBack(aCallback, aResult);
                                });
                            }
                        } catch (e) {
                            alert(aData.error);
                        }
                    }
                }, error: function (aCallbackData) {
                    var aResult = { State: 0, Datas: aCallbackData.statusText };
                    alert("acHttpGet " + aCallbackData.status + ", " + aCallbackData.statusText);
                    hhls.callBack(aCallback, aResult);
                }
            });
        }
        catch (E) {
            alert(E);
        }
    },
    acGetData: function (aUrl, aData, aType, aCallBack) {
        try {
            aUrl += aUrl.indexOf("?") > 0 ? "&" : "?" + "randomtm=" + Math.random();
            $.ajax({
                url: aUrl,
                data: aData,
                type: aType,
                dataType: 'json',
                jsonp: 'callback',
                timeout: 50000,
                success: function (aCallbackData, b, c) {
                    hhls.callBack(aCallBack, aCallbackData);
                },
                error: function (a, b, c) {
                    var aResult = { State: 0, Datas: { Ea: a, Eb: b, Ec: c } };
                    hhls.callBack(aCallBack, aResult);
                }
            });
        }
        catch (e) {; }
    },
    acExecuteSql: function (aPath, aDataPs, aCallback) {
        try {
            var aPostPs = {
                Path: aPath,
                Ps: hhls.getObjJson(aDataPs)
            };
            Ac.Call_Get("acExecuteSql", aPostPs, function (aRes) {
                hhls.callBack(aCallback, aRes);
            });
        }
        catch (E) {
            alert(E);
        }
    },
    Call_Get: function (aAction, aPs, aCallback) {
        try {
            var aUrl = Ac.Info.SvcUrl;
            //aUrl += aUrl.indexOf("?") > 0 ? "&" : "?";
            //aUrl += "Action=" + aAction;
            //aUrl += "&rnd=" + Math.random();
            $.ajax({
                url: aUrl,
                data: aPs,
                type: 'GET',
                timeout: 50000,
                success: function (aCallbackData, b, c) {
                    var aResult = hhls.getJsonObj(aCallbackData);
                    hhls.callBack(aCallback, aResult);
                },
                error: function (a, b, c) {
                    var aResult = { State: 0, Datas: { Ea: a, Eb: b, Ec: c } };
                    hhls.callBack(aCallback, aResult);
                }
            });
        }
        catch (E) {; }
    },
};