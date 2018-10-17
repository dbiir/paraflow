function updateClusterInfo() {
    $.getJSON("http://127.0.0.1:8080/info", function (info) {
        $('#version-number').text(info.version);
        $('#uptime').text(info.uptime);
        $('#status-indicator').removeClass("text-info").removeClass("text-danger").addClass("text-info");
    }).fail(function() {
        $('#status-indicator').removeClass("text-info").removeClass("text-danger").addClass("text-danger");
    });
}