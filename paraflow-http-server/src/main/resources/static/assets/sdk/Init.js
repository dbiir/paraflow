/// <reference path="../../Libs/sdk/jQuery-2.1.3.min.js" />
/// <reference path="../../Libs/sdk/json.js" />
/// <reference path="../../Libs/sdk/baiduTpls.js" />
/// <reference path="../../Libs/sdk/date.js" />
/// <reference path="../../Libs/sdk/hhls.js" />
/// <reference path="../../Libs/sdk/hhac.js" />

var Init = {
    //数据
    Datas: {},
    //sql语句的路径
    Path: {},
    // 请求的url
    Url: {
        acGetTables: "http://localhost:2018/acGetTables",
        acQuery: "http://localhost:2018/acQuery",
    },
    //web弹出框样式
    Utility: {
        WebToast: "<div id=\"webToast\">"
            + "<div class=\"web_transparent\"></div>"
            + "<div class=\"web-toast\">"
            + "<div class=\"sk-spinner sk-spinner-three-bounce\">"
            + "<div class=\"sk-bounce1\"></div>"
            + "<div class=\"sk-bounce2\"></div>"
            + "<div class=\"sk-bounce3\"></div>"
            + "</div>"
            + "<p class=\"web-toast_content\">数据加载中</p>"
            + "</div>"
            + "</div>",
    },
    //Toast Style
    Utility: {
        WxToast: "<div id=\"wxToast\">"
            + "<div class=\"wx_transparent\"></div>"
            + "<div class=\"wx-toast\">"
            + "<div class=\"sk-spinner sk-spinner-three-bounce\">"
            + "<div class=\"sk-bounce1\"></div>"
            + "<div class=\"sk-bounce2\"></div>"
            + "<div class=\"sk-bounce3\"></div>"
            + "</div>"
            + "<p class=\"wx-toast_content\">数据加载中</p>"
            + "</div>"
            + "</div>",
        WebToast: "<div id=\"webToast\">"
            + "<div class=\"web_transparent\"></div>"
            + "<div class=\"web-toast\">"
            + "<div class=\"sk-spinner sk-spinner-three-bounce\">"
            + "<div class=\"sk-bounce1\"></div>"
            + "<div class=\"sk-bounce2\"></div>"
            + "<div class=\"sk-bounce3\"></div>"
            + "</div>"
            + "<p class=\"web-toast_content\">数据加载中</p>"
            + "</div>"
            + "</div>",
        Loading: "<div class='ibox'><div class='ibox-content'><div class='sk-spinner sk-spinner-three-bounce'><div class='sk-bounce1'></div><div class='sk-bounce2'></div><div class='sk-bounce3'></div></div></div></div>",
    },
    //web Toast
    WebToast: function (aContent) {
        var me = Init;
        try {
            $("body").append(me.Utility.WebToast);
            var w = $(window).width();
            var aW = $(".web-toast").width();
            var left = (w - aW) / 2;
            $(".web-toast").css("left", left + "px");
            $(".web-toast_content").text(aContent);
        } catch (e) {
            ;
        }
    },
    WxToast: function (aContent) {
        var me = Init;
        try {
            $("body").append(me.Utility.WxToast);
            var w = $(window).width();
            var aW = $(".wx-toast").width();
            var left = (w - aW) / 2;
            $(".wx-toast").css("left", left + "px");
            $(".wx-toast_content").text(aContent);
        } catch (e) {
            ;
        }
    },
    //Toast
    Web_Toast: function (aContent, aTimeOut) {
        var me = Init;
        try {
            me.WebToast(aContent);
            me.ClearToast("#webToast", aTimeOut);
        } catch (e) {
            ;
        }
    },
    Wx_Toast: function (aContent, aTimeOut) {
        var me = Init;
        try {
            me.WxToast(aContent);
            me.ClearToast("#wxToast", aTimeOut);
        } catch (e) {
            ;
        }
    },
    //clear Toast, set time
    ClearToast: function (aElement, aTimeOut) {
        var me = Init;
        try {
            setTimeout(function () {
                $(aElement).remove();
            }, aTimeOut * 1000);
        } catch (e) {
            ;
        }
    },
    //load Pciture
    LoadWxImg: function () {
        var me = Init;
        try {
            var aImgs = $(".WxImg");
            $.each(aImgs, function (aInd, aItem) {
                try {
                    var aImg = $(aItem);
                    var aKey = aImg.attr("Key");
                    if (aKey.length > 0) {
                        var aUrl = me.Datas.TomcatUrl + aKey + ".jpg";
                        aImg.attr("src", aUrl);
                    }
                } catch (ee) {
                    ;
                }
            });
        } catch (e) {
            ;
        }
    },
    //load Pciture
    LoadWxImg: function (aSelector) {
        var me = Init;
        try {
            var aImgs = $(aSelector);
            $.each(aImgs, function (aInd, aItem) {
                try {
                    var aImg = $(aItem);
                    var aKey = aImg.attr("Key");
                    if (aKey.length > 0) {
                        var aUrl = me.Datas.TomcatUrl + aKey;
                        aImg.attr("src", aUrl);
                    }
                } catch (ee) {
                    ;
                }
            });
        } catch (e) {
            ;
        }
    },
    doPlayVoice: function (aId) {
        var me = Init;
        try {
            var aDlg = $("#dlgPlayMp3").unbind("shown.bs.modal").bind("shown.bs.modal", function () {
                try {
                    var aUrl = me.Datas.MediaPath + Common.Config.DataSvc.WxSrc + "/" + aId + ".mp3";
                    var aHtml = '<audio controls="controls" autoplay="autoplay" style="width:100%"><source src="' + aUrl + '" type="audio/mpeg" />Your browser does not support the audio element.</audio>';
                    hhls.fillElement("#divPlayer", aHtml);
                } catch (e) {
                    ;
                }
            }).unbind("hidden.bs.modal").bind("hidden.bs.modal", function () {
                try {
                    hhls.clearElement("#divPlayer");
                } catch (e) {
                    ;
                }
            }).modal("show");
        } catch (e) {
            ;
        }
    }

}