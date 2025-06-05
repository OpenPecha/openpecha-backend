class YamlUploadHandler {
    constructor() {
        this.FILE_SIZE_UNITS = ['Bytes', 'KB', 'MB', 'GB'];
        this.TOAST_DURATION = 3000;
        this.selectedFile = null;
        this.selectedDestination = 'pecha'; // Default destination
        
        // Cache DOM elements
        this.elements = {
            uploadArea: document.getElementById('upload-area'),
            fileInput: document.getElementById('file-input'),
            browseButton: document.getElementById('browse-button'),
            fileInfo: document.getElementById('file-info'),
            previewContent: document.getElementById('preview-content'),
            detailsContent: document.getElementById('details-content'),
            clearButton: document.getElementById('clear-button'),
            uploadButton: document.getElementById('upload-button'),
            toast: document.getElementById('toast'),
            destinationOptions: document.querySelectorAll('input[name="destination"]')
        };
        
        this.initialize();
    }
    
    async initialize() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
            this.setupEventListeners();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }

    
    setupEventListeners() {
        // Drag and drop handlers with bound context
        this.elements.uploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
        this.elements.uploadArea.addEventListener('dragleave', this.handleDragLeave.bind(this));
        this.elements.uploadArea.addEventListener('drop', this.handleDrop.bind(this));
        
        // Click handlers
        this.elements.uploadArea.addEventListener('click', () => this.elements.fileInput.click());
        this.elements.browseButton.addEventListener('click', (e) => {
            e.stopPropagation();
            this.elements.fileInput.click();
        });
        
        // File selection and actions
        this.elements.fileInput.addEventListener('change', () => this.handleFiles(this.elements.fileInput.files));
        this.elements.clearButton.addEventListener('click', this.clearSelection.bind(this));
        this.elements.uploadButton.addEventListener('click', this.uploadFile.bind(this));
        
        // Destination selection
        this.elements.destinationOptions.forEach(option => {
            option.addEventListener('change', (e) => {
                this.selectedDestination = e.target.value;
                console.log(`Destination selected: ${this.selectedDestination}`);
                this.updateFileDetailsWithDestination();
            });
        });
    }
    
    handleDragOver(e) {
        e.preventDefault();
        this.elements.uploadArea.classList.add('dragover');
    }
    
    handleDragLeave(e) {
        e.preventDefault();
        this.elements.uploadArea.classList.remove('dragover');
    }
    
    handleDrop(e) {
        e.preventDefault();
        this.elements.uploadArea.classList.remove('dragover');
        this.handleFiles(e.dataTransfer.files);
    }
    
    handleFiles(files) {
        if (!files.length) return;
        
        const file = files[0];
        
        // Validate file type
        if (!this.isYamlFile(file.name)) {
            this.showToast('Please select a YAML file (.yaml or .yml)', 'error');
            this.showUploadError();
            return;
        }
        
        this.selectedFile = file;
        this.elements.uploadButton.disabled = false;
        
        // Update UI with file details
        this.elements.fileInfo.style.display = 'grid';
        this.displayFileDetails(file);
        
        // Preview file content
        this.readFileContent(file);
    }
    
    isYamlFile(filename) {
        return /\.(yaml|yml)$/i.test(filename);
    }
    
    showUploadError() {
        this.elements.uploadArea.classList.add('error');
        setTimeout(() => this.elements.uploadArea.classList.remove('error'), this.TOAST_DURATION);
    }
    
    readFileContent(file) {
        const reader = new FileReader();
        reader.onload = e => this.elements.previewContent.textContent = e.target.result;
        reader.readAsText(file);
    }
    
    displayFileDetails(file) {
        this.elements.detailsContent.innerHTML = '';
        
        const details = [
            { label: 'File Name', value: file.name },
            { label: 'File Type', value: file.type || 'text/yaml' },
            { label: 'File Size', value: this.formatFileSize(file.size) },
            { label: 'Last Modified', value: new Date(file.lastModified).toLocaleString() },
            { label: 'Destination', value: this.getDestinationDisplayName(), id: 'destination-detail' }
        ];
        
        const fragment = document.createDocumentFragment();
        
        details.forEach(detail => {
            const labelElement = document.createElement('div');
            labelElement.className = 'label';
            labelElement.textContent = detail.label;
            
            const valueElement = document.createElement('div');
            valueElement.className = 'value';
            valueElement.textContent = detail.value;
            if (detail.id) {
                valueElement.id = detail.id;
            }
            
            fragment.appendChild(labelElement);
            fragment.appendChild(valueElement);
        });
        
        this.elements.detailsContent.appendChild(fragment);
    }
    
    /**
     * Updates the file details display with the current destination
     */
    updateFileDetailsWithDestination() {
        const destinationElement = document.getElementById('destination-detail');
        if (destinationElement) {
            destinationElement.textContent = this.getDestinationDisplayName();
        }
    }
    
    /**
     * Gets a user-friendly display name for the selected destination
     * @returns {string} Display name for the destination
     */
    getDestinationDisplayName() {
        return this.selectedDestination === 'fodian' ? 'fodian.org (Chinese texts)' : 'pecha.org (Tibetan texts)';
    }
    
    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        
        const k = 1024;
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + this.FILE_SIZE_UNITS[i];
    }
    
    clearSelection() {
        this.selectedFile = null;
        this.elements.fileInput.value = '';
        this.elements.fileInfo.style.display = 'none';
        this.elements.previewContent.textContent = '';
        this.elements.detailsContent.innerHTML = '';
        this.elements.uploadButton.disabled = true;
    }
    
    uploadFile() {
        if (!this.selectedFile) return;
        
        const formData = new FormData();
        formData.append('file', this.selectedFile);
        formData.append('destination', this.selectedDestination);
        
        // Add destination to file details display
        this.updateFileDetailsWithDestination();
        
        this.elements.uploadButton.disabled = true;
        this.elements.uploadButton.textContent = 'Uploading...';
        
        // Get the appropriate endpoint based on destination
        const endpoint = this.selectedDestination === 'fodian' 
            ? `${this.API_ENDPOINT}/categories/fodian` 
            : `${this.API_ENDPOINT}/categories`;
            
        fetch(endpoint, {
            method: 'PUT',
            body: formData
        })
        .then(response => {
            console.log("Response :: ",response);
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            console.log(data);
            this.showToast(data.message);
            this.clearSelection();
        })
        .catch(error => {
            this.showToast('Upload failed!', 'error');
            console.error('Error:', error);
        })
        .finally(() => {
            this.elements.uploadButton.disabled = false;
            this.elements.uploadButton.textContent = 'Upload';
        });
    }
    
    showToast(message, type = 'success') {
        const toast = this.elements.toast;
        toast.textContent = message;
        toast.className = 'toast show';
        
        if (type === 'error') {
            toast.classList.add('error');
        }
        
        setTimeout(() => toast.classList.remove('show'), this.TOAST_DURATION);
    }
}

document.addEventListener('DOMContentLoaded', function() {
    // Initialize the upload handler
    new YamlUploadHandler();
});