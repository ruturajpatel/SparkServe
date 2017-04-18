
$(document).ready(function () {


    $('#input_prog_file').on("change", function(){ 
        var file = this.files[0];
        var flag = false;
        name = file.name;
        size = file.size;
        type = file.type;
        var extension = name.replace(/^.*\./, '');
        if (file.name.length < 1) {
            alert("Please select a file");
            flag = true;

        }
        else if (file.size > 100000) {
            alert("The file is too big");
            flag=true;

        }
        else if(extension != 'py' ) {
            alert("We only support zip files for now");
            flag = true;

        }
        else {
            $('.btnFile').attr("disabled", false);

        }

        if(flag == true){
            $(this).val('');
        }
    });

    $('#input_data_file').on("change", function(){ 
        var file = this.files[0];
        var flag = false;
        name = file.name;
        size = file.size;
        type = file.type;
        var extension = name.replace(/^.*\./, '');
        if (file.name.length < 1) {
            alert("Please select a file");
            flag = true;

        }
        
        else if(extension != 'zip' || extension != 'txt' || extension != 'csv' ) {
            alert("We only support zip/txt/csv files for now");
            flag = true;

        }
        else {
            $('.btnFile').attr("disabled", false);

        }

        if(flag == true){
            $(this).val('');
        }
    });

    $('.btnFile').click(function (e) {
                e.preventDefault();

                var formData = new FormData($('#forminput')[0]);
                $.ajax({
                    url: 'api/uploadcode',
                    type: 'POST',
                    
                    success: completeHandler = function (data) {
                      
                        $(".upload-prog-status").html("File Uploaded");
                        $(".upload-prog-status").css('color','green');
                        getProgFiles();
                    },
                    error: errorHandler = function (error1) {
                        $(".upload-prog-status").html("File Upload failed");
                        $(".upload-prog-status").css('color','red');
                    },
                    // Form data
                    data: formData,
                    // Options to tell jQuery not to process data or worry about the content-type
                    cache: false,
                    contentType: false,
                    processData: false
                }, 'json')

                return false;
    });

    $('.btnDataFile').click(function (e) {
        e.preventDefault();
        debugger;
        var formData = new FormData($('#formdatainput')[0]);
        $.ajax({
            url: 'api/uploaddata',
            type: 'POST',
            success: completeHandler = function (data) {
                      
                        $(".upload-data-status").html("File Uploaded");
                        $(".upload-data-status").css('color','green');
                        getDataFiles();
                    },
                    error: errorHandler = function (error1) {
                        $(".upload-data-status").html("File Upload failed");
                        $(".upload-data-status").css('color','red');
                    },
            // Form data
            data: formData,
            // Options to tell jQuery not to process data or worry about the content-type
            cache: false,
            contentType: false,
            processData: false
        }, 'json')

        return false;
    });
})
