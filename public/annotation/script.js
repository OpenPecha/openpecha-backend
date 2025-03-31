class AnnotationForm {
    constructor() {
        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.form = document.getElementById('annotationForm');
        this.annotationSelect = document.getElementById('annotation');
        this.pechaSelect = document.getElementById('pecha');
        this.pechaDropdown = document.getElementById('pechaDropdown');
        this.segmentationLayer = document.getElementById('segmentationLayer');
        this.segmentationTitle = document.getElementById('segmentationTitle');
        this.googleDocsUrl = document.getElementById('googleDocsUrl');
        this.toastContainer = document.getElementById('toastContainer');

        this.metadata = null;
        // Bind methods to maintain 'this' context
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleAnnotationChange = this.handleAnnotationChange.bind(this);
        this.initializeForm = this.initializeForm.bind(this);

        // Initialize event listeners
        this.setupEventListeners();
        this.initializeForm();
    }

    setupEventListeners() {
        this.form.addEventListener('submit', this.handleSubmit);
        this.pechaSelect.addEventListener('change', (e) => this.fetchMetadata(e.target.value));
        this.annotationSelect.addEventListener('change', this.handleAnnotationChange);
    }

    async toggleConditionalFields() {
        const isPechaCommentary = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);

        const isSegmentation = this.annotationSelect?.value === 'Segmentation';

        if (!isPechaCommentary || !isSegmentation) {
            this.pechaDropdown.value = "";
            this.segmentationLayer.value = "";
        }
        const fields = {
            'pechaField': isPechaCommentary && isSegmentation,
            'segmentationField': isSegmentation && isPechaCommentary
        };

        Object.entries(fields).forEach(([fieldId, shouldShow]) => {
            document.getElementById(fieldId).style.display = shouldShow ? 'block' : 'none';
        });

        // Reset validation state
        this.form.classList.remove('was-validated');
    }

    async fetchMetadata(pechaId) {
        try {
            // Await the fetch call to get the response
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${pechaId}`, {
                method: 'GET',
                headers: {
                    'accept': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const metadata = await response.json();
            this.metadata = metadata;
        } catch (error) {
            console.error('Error fetching metadata:', error);
            throw error;
        } finally {
            this.toggleConditionalFields();
        }
    }

    handleAnnotationChange(event) {
        this.toggleConditionalFields();
    }

    async fetchPechaList(filterBy) {
        let body = { filter: {} };
        const filters = {
            "commentary_of": {
                "and": [
                    { "field": "commentary_of", "operator": "==", "value": null },
                    { "field": "translation_of", "operator": "==", "value": null }
                ]
            },
            "version_of": {
                "and": [
                    { "field": "commentary_of", "operator": "==", "value": null },
                    { "field": "version_of", "operator": "==", "value": null }
                ]
            },
            "translation_of": {
                "field": "language",
                "operator": "==",
                "value": "bo"
            }
        };

        body.filter = filters[filterBy] || {};

        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body)
            });
            if (!response.ok) {
                throw new Error(`Failed to fetch data: ${response.statusText}`);
            }
            const pechas = await response.json();
            return pechas;
        } catch (error) {
            this.handleSpinner(this.pechaOptionsContainer, false);
            console.error("Error loading pecha options:", error);
            alert("Unable to load pecha options. Please try again later.");
        }
    }

    populatePechaDropdowns(pechas) {
        pechas.forEach(pecha => {
            const option = new Option(`${pecha.id} - ${pecha.title}`, pecha.id);
            this.pechaSelect.add(option.cloneNode(true));
            this.pechaDropdown.add(option);
        });
    }

    async initializeForm() {
        try {
            const pechas = await this.fetchPechaList("version_of");
            this.populatePechaDropdowns(pechas);
        } catch (error) {
            console.error('Error initializing form:', error);
        }
    }

    getFormData() {
        const formData = new FormData(this.form);
        return Object.fromEntries(formData.entries());
    }

    resetForm() {
        this.form.reset();
        this.toggleConditionalFields('');
    }

    validateForm(data) {
        if (!data.pecha) {
            this.highlightError(this.pechaSelect);
            this.showToast('Pecha is required', 'error');
            return false;
        }
        if (!data.annotation) {
            this.highlightError(this.annotationSelect);
            this.showToast('Annotation is required', 'error');
            return false;
        }
        if (!data.pechaDropdown) {
            this.highlightError(this.pechaDropdown);
            this.showToast('Pecha is required', 'error');
            return false;
        }
        // if (!data.segmentationLayer) {
        //     this.highlightError(this.segmentationLayer);
        //     this.showToast('Segmentation Layer is required', 'error');
        //     return false;
        // }

        if (!data.segmentationTitle) {
            this.highlightError(this.segmentationTitle);
            this.showToast('Segmentation Title is required', 'error');
            return false;
        }
        
        if (!data.googleDocsUrl) {
            this.highlightError(this.googleDocsUrl);
            this.showToast('Google Docs URL is required', 'error');
            return false;
        }
        return true;
    }

    async submitAnnotation(data) {
        const response = await fetch(`${this.API_ENDPOINT}/annotation/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        });

        if (!response.ok) {
            throw new Error('Failed to add annotation');
        }

        return response.json();
    }

    async handleSubmit(event) {
        event.preventDefault();

        try {
            const data = this.getFormData();
            console.log("data ::: ", data)
            const isValid = this.validateForm(data);
            if (!isValid) {
                return;
            }
            await this.submitAnnotation(data);

            this.showToast('Annotation added successfully!', 'success');
            this.resetForm();
        } catch (error) {
            this.showToast(error.message, 'error');
            console.error('Error submitting form:', error);
        }
    }



    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `${this.getToastIcon(type)} ${message}`;
        this.toastContainer.appendChild(toast);

        setTimeout(() => {
            toast.remove();
        }, 3000);
    }

    getToastIcon(type) {
        switch (type) {
            case 'success':
                return '<i class="fas fa-check-circle"></i>';
            case 'error':
                return '<i class="fas fa-exclamation-circle"></i>';
            default:
                return '<i class="fas fa-info-circle"></i>';
        }
    }

    highlightError(field) {
        field.classList.add('error');
    }
}

// Initialize the form when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new AnnotationForm();
});
