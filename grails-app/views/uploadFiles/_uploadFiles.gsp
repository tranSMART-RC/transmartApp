<script type="text/javascript" src="${resource(dir:'js', file:'fine-uploader/iframe.xss.response-3.7.1.js')}"></script>
<script type="text/javascript" src="${resource(dir:'js', file:'fine-uploader/jquery.fineuploader-3.7.1.js')}"></script>
<script type="text/javascript" src="${resource(dir:'js', file:'fine-uploader/jquery.fineuploader-3.7.1.min.js')}"></script>
<link rel="stylesheet" type="text/css" href="${resource(dir:'css', file:'fineuploader-3.5.0.css')}">
<div id="uploadtitle"><p>Upload files in folder ${parentFolder?.folderName}</p></div>
<div id="fine-uploader-basic" class="btn btn-success">
  <i class="icon-upload icon-white"></i> <p>To upload files, click or drag files in this zone.</p>
</div>

<table style="width: 100%;" class="uploadtable" id="uploadtable">
</table>

 <form name="form">
    <input type="hidden" name="parent" id="parentFolderId" value="${parentFolder?.id}" />
    <input type="hidden" name="parentName" id="parentFolderName" value="${parentFolder?.folderName}" />
    <input type="hidden" name="existingfiles" id="existingfiles" value="yes" />
</form>
<script>
jQuery(document).ready(function() {
    $fub = jQuery('#fine-uploader-basic');

    var uploader = new qq.FineUploaderBasic({
      button: $fub[0],
      multiple: true,
      request: {
        endpoint: 'uploadFiles/upload?parentId='+jQuery('#parentFolderId').val()+'&folderParentId='+jQuery('#parentId').val()
      },
      callbacks: {
        onSubmit: function(id, fileName) {
            var folderName = jQuery('#folderName').val();
              
            jQuery('#uploadtable').append('<tr id="file-' + id + '" class="alert" style="margin: 20px 0 0">'+
                '<td id="parent">'+folderName+'</td>'+
                '<td id="name">'+fileName+'</td>'+
                '<td id="status">Submitting</td>'+
                '<td id="progress"></td></tr>');
        },
        onUpload: function(id, fileName) {
            jQuery('#file-' + id + " #name").html(fileName);
            jQuery('#file-' + id + " #status").html('Initializing ');
        },
        onProgress: function(id, fileName, loaded, total) {
          if (loaded < total) {
            progress = Math.round(loaded / total * 100) + '% of ' + Math.round(total / 1024) + ' kB';

            jQuery('#file-' + id + " #status").html('Uploading ');
            jQuery('#file-' + id + " #progress").html(progress);
          } else {
              jQuery('#file-' + id + " #status").html('Saving');
              jQuery('#file-' + id + " #progress").html('100%');
          }
        },
        onComplete: function(id, fileName, responseJSON) {
          if (responseJSON.success) {
            jQuery('#file-' + id + " #status").html('File successfully uploaded ');
              jQuery('#file-' + id + " #progress").html('');

              var folderParentId=responseJSON.folderParentId;
              var folderId=responseJSON.folderId;
              incrementeDocumentCount(folderId);
              
              if(folderId == jQuery('#parentFolderId').val()){
                jQuery('#metadata-viewer').empty().addClass('ajaxloading');
                jQuery('#metadata-viewer').load(folderDetailsURL + '?id=' + folderId, {}, function() {
                    jQuery('#metadata-viewer').removeClass('ajaxloading');
                });
              }
          } else {
              jQuery('#file-' + id + " #status").html('Error: '+responseJSON.error);
                jQuery('#file-' + id + " #progress").html('');
          }
          
        }
      }
    });
  });
</script>
