document.addEventListener('DOMContentLoaded', function() {
    // DOM elements
    const uploadArea = document.getElementById('upload-area');
    const fileInput = document.getElementById('file-input');
    const browseButton = document.getElementById('browse-button');
    const fileInfo = document.getElementById('file-info');
    const previewContent = document.getElementById('preview-content');
    const detailsContent = document.getElementById('details-content');
    const clearButton = document.getElementById('clear-button');
    const uploadButton = document.getElementById('upload-button');
    const toast = document.getElementById('toast');
    
    let selectedFile = null;

    // Event listeners for drag and drop
    uploadArea.addEventListener('dragover', function(e) {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    });

    uploadArea.addEventListener('dragleave', function(e) {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
    });

    uploadArea.addEventListener('drop', function(e) {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        
        const files = e.dataTransfer.files;
        handleFiles(files);
    });

    // Click events
    uploadArea.addEventListener('click', function() {
        fileInput.click();
    });

    browseButton.addEventListener('click', function(e) {
        e.stopPropagation();
        fileInput.click();
    });

    fileInput.addEventListener('change', function() {
        handleFiles(this.files);
    });

    clearButton.addEventListener('click', clearSelection);
    
    uploadButton.addEventListener('click', uploadFile);

    // Function to handle selected files
    function handleFiles(files) {
        if (files.length === 0) return;
        
        const file = files[0];
        
        // Check if the file is a YAML file
        if (!file.name.match(/\.(yaml|yml)$/i)) {
            showToast('Please select a YAML file (.yaml or .yml)', 'error');
            uploadArea.classList.add('error');
            setTimeout(() => {
                uploadArea.classList.remove('error');
            }, 3000);
            return;
        }

        selectedFile = file;
        uploadButton.disabled = false;
        
        // Display file information
        fileInfo.style.display = 'grid';
        displayFileDetails(file);
        
        // Read and display file content
        const reader = new FileReader();
        reader.onload = function(e) {
            const content = e.target.result;
            previewContent.textContent = content;
        };
        reader.readAsText(file);
    }

    // Function to display file details
    function displayFileDetails(file) {
        detailsContent.innerHTML = '';
        
        const details = [
            { label: 'File Name', value: file.name },
            { label: 'File Type', value: file.type || 'text/yaml' },
            { label: 'File Size', value: formatFileSize(file.size) },
            { label: 'Last Modified', value: new Date(file.lastModified).toLocaleString() }
        ];
        
        details.forEach(detail => {
            const labelElement = document.createElement('div');
            labelElement.className = 'label';
            labelElement.textContent = detail.label;
            
            const valueElement = document.createElement('div');
            valueElement.className = 'value';
            valueElement.textContent = detail.value;
            
            detailsContent.appendChild(labelElement);
            detailsContent.appendChild(valueElement);
        });
    }

    // Function to format file size
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    // Function to clear selection
    function clearSelection() {
        selectedFile = null;
        fileInput.value = '';
        fileInfo.style.display = 'none';
        previewContent.textContent = '';
        detailsContent.innerHTML = '';
        uploadButton.disabled = true;
    }

    // Function to upload file (simulated)
    function uploadFile() {
        if (!selectedFile) return;
        
        // Simulate upload process
        uploadButton.disabled = true;
        uploadButton.textContent = 'Uploading...';
        
        // Simulate API call with timeout
        setTimeout(() => {
            uploadButton.textContent = 'Upload';
            showToast('File uploaded successfully!');
            clearSelection();
        }, 2000);
    }

    // Function to show toast notification
    function showToast(message, type = 'success') {
        toast.textContent = message;
        toast.className = 'toast show';
        
        if (type === 'error') {
            toast.classList.add('error');
        }
        
        setTimeout(() => {
            toast.classList.remove('show');
        }, 3000);
    }
});