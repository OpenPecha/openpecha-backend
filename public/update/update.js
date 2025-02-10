class UpdateMetaData {
    constructor() {
        this.elements = {
            form: document.getElementById('updateForm'),
            pechaSelect: document.getElementById('pechaOptions'),
            docsInput: document.getElementById('googleDocsInput'),
            updateButton: document.getElementById('updateButton'),
            toastContainer: document.getElementById('toastContainer')
        };

        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.setupEventListeners();
        this.fetchPechaOptions();
    }

    setupEventListeners() {
        this.elements.form.addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleUpdate();
        });
    }

    async fetchPechaOptions() {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/pecha/`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

            const pechas = await response.json();
            this.updatePechaOptions(pechas);
        } catch (error) {
            console.error('Error loading pecha options:', error);
            this.showToast('Unable to load pecha options. Please try again later.', 'error');
        }
    }

    updatePechaOptions(pechas) {
        // Clear existing options except the first one
        this.elements.pechaSelect.innerHTML = '<option value="">Select pecha</option>';

        // Use DocumentFragment for better performance
        const fragment = document.createDocumentFragment();

        pechas.forEach(({ id, title }) => {
            const option = document.createElement('option');
            option.value = id;
            option.textContent = `(${id}) ${title}`;
            fragment.appendChild(option);
        });

        this.elements.pechaSelect.appendChild(fragment);
    }

    validateFields() {
        const publishTextId = this.elements.pechaSelect.value.trim();
        const googleDocLink = this.elements.docsInput.value.trim();

        if (!publishTextId) {
            this.showToast('Please select the published text', 'warning');
            return false;
        }

        const docId = this.extractDocIdFromLink(googleDocLink);
        if (!docId) {
            this.showToast('Enter valid Google Docs link', 'warning');
            return false;
        }

        return { publishTextId, docId };
    }

    async handleUpdate() {
        const validatedData = this.validateFields();
        if (!validatedData) return;

        try {
            const { publishTextId, docId } = validatedData;
            const blob = await downloadDoc(docId);
            if (!blob) {
                this.showToast("Failed to download document","error")
                throw new Error('Failed to download document');
            }

            await this.uploadDocument(publishTextId, blob, docId);
            this.showToast('Document updated successfully!', 'success');
            this.elements.form.reset();
        } catch (error) {
            console.error('Error during update:', error);
            this.showToast(`Error: ${error.message}`, 'error');
        }
    }

    async uploadDocument(publishTextId, blob, docId) {
        const formData = new FormData();
        formData.append('text', blob, `text_${docId}.docx`);
        formData.append('id', publishTextId);

        const response = await fetch(`${this.API_ENDPOINT}/update-text/`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            const errorText = await response.text();
            this.showToast("Update failed","error")
            throw new Error(`Update failed: ${errorText}`);
        }
        return response;
    }

    extractDocIdFromLink(docLink) {
        const regex = /\/d\/([a-zA-Z0-9-_]+)/;
        const match = docLink.match(regex);
        return match?.[1] || null;
    }

    showToast(message, type) {
        this.clearToasts();

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;

        this.elements.toastContainer.appendChild(toast);
        setTimeout(() => toast.remove(), 3000);
    }

    clearToasts() {
        this.elements.toastContainer.innerHTML = '';
    }
}


document.addEventListener('DOMContentLoaded', () => new UpdateMetaData());