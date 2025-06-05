class AnnotationForm {
    constructor() {
        this.form = document.getElementById('annotationForm');
        this.annotationSelect = document.getElementById('annotation');
        this.pechaSelect = document.getElementById('pecha');
        this.pechaDropdownLabel = document.getElementById('pechaDropdownLabel');
        this.pechaDropdown = document.getElementById('pechaDropdown');
        this.parentAnnotation = document.getElementById('parentAnnotation');
        this.annotationTitle = document.getElementById('annotationTitle');
        this.toastContainer = document.getElementById('toastContainer');
        this.pechaLoadingSpinner = document.getElementById('pechaLoadingSpinner');
        
        // Existing annotations container elements
        this.existingAnnotationsContainer = document.getElementById('existingAnnotationsContainer');
        this.existingAnnotationsList = document.getElementById('existingAnnotationsList');
        this.annotationsLoadingSpinner = document.getElementById('annotationsLoadingSpinner');

        // Search-related elements
        this.searchContainers = document.querySelectorAll('.select-search-container');

        this.metadata = null;
        this.selectedAnnotation = null;
        
        // Bind methods to maintain 'this' context
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleAnnotationChange = this.handleAnnotationChange.bind(this);
        this.initializeForm = this.initializeForm.bind(this);
        this.initializeSearchUI = this.initializeSearchUI.bind(this);
        this.toggleLoadingSpinner = this.toggleLoadingSpinner.bind(this);
        this.toggleAnnotations = this.toggleAnnotations.bind(this);
        this.selectAnnotationForUpdate = this.selectAnnotationForUpdate.bind(this);

        // Initialize event listeners
        this.initialize();
    }

    async initialize() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
            this.setupEventListeners();
            this.initializeForm();
            this.initializeSearchUI();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }
    
    setupEventListeners() {
        this.form.addEventListener('submit', this.handleSubmit);
        this.pechaSelect.addEventListener('change', (e) => this.onPechaSelect(e.target.value));
        this.annotationSelect.addEventListener('change', this.handleAnnotationChange);
        
        // Add event listener for the toggle annotations button
        const toggleBtn = document.getElementById('toggleAnnotationsBtn');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', this.toggleAnnotations);
        }
        
        // Add event listener for the entire header to toggle annotations
        const annotationsHeader = document.querySelector('.annotations-header');
        if (annotationsHeader) {
            annotationsHeader.addEventListener('click', this.toggleAnnotations);
        }
    }
    
    // Method to toggle the visibility of the annotations list
    toggleAnnotations(event) {
        // Prevent event propagation if the click is on the toggle button
        if (event && event.target && (event.target.id === 'toggleAnnotationsBtn' || event.target.id === 'toggleIcon')) {
            event.stopPropagation();
        }
        
        const annotationsList = this.existingAnnotationsList;
        const toggleBtn = document.getElementById('toggleAnnotationsBtn');
        const toggleIcon = document.getElementById('toggleIcon');
        
        if (annotationsList && toggleBtn && toggleIcon) {
            annotationsList.classList.toggle('collapsed');
            toggleBtn.classList.toggle('collapsed');
            
            // Store the collapsed state in local storage for persistence
            const isCollapsed = annotationsList.classList.contains('collapsed');
            localStorage.setItem('annotationsCollapsed', isCollapsed);
        }
    }

    // New method to initialize search functionality
    initializeSearchUI() {
        this.searchContainers.forEach(container => {
            const select = container.querySelector('select');
            const searchOverlay = container.querySelector('.search-overlay');
            const searchInput = container.querySelector('.search-input');
            const searchResults = container.querySelector('.search-results');

            // Prevent the native dropdown from showing
            select.addEventListener('mousedown', (e) => {
                e.preventDefault();
                searchOverlay.classList.toggle('active');
                if (searchOverlay.classList.contains('active')) {
                    searchInput.focus();
                    this.populateSearchResults(select, searchResults, searchInput.value);
                }
            });

            // Close search overlay when clicking outside
            document.addEventListener('click', (e) => {
                if (!container.contains(e.target)) {
                    searchOverlay.classList.remove('active');
                }
            });

            // Search functionality
            searchInput.addEventListener('input', () => {
                this.populateSearchResults(select, searchResults, searchInput.value);
            });

            // Select an option from search results
            searchResults.addEventListener('click', (e) => {
                if (e.target.classList.contains('search-item')) {
                    const value = e.target.dataset.value;
                    select.value = value;

                    // Trigger change event
                    const changeEvent = new Event('change', { bubbles: true });
                    select.dispatchEvent(changeEvent);

                    searchOverlay.classList.remove('active');
                }
            });

            // Handle keyboard navigation
            searchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') {
                    searchOverlay.classList.remove('active');
                } else if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'down');
                } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    this.navigateSearchResults(searchResults, 'up');
                } else if (e.key === 'Enter') {
                    e.preventDefault();
                    const selectedItem = searchResults.querySelector('.search-item.selected');
                    if (selectedItem) {
                        const value = selectedItem.dataset.value;
                        select.value = value;

                        // Trigger change event
                        const changeEvent = new Event('change', { bubbles: true });
                        select.dispatchEvent(changeEvent);

                        searchOverlay.classList.remove('active');
                    }
                }
            });
        });
    }

    // Helper method to populate search results
    populateSearchResults(select, resultsContainer, searchTerm) {
        resultsContainer.innerHTML = '';
        searchTerm = searchTerm.toLowerCase();

        Array.from(select.options).forEach(option => {
            if (option.value && (searchTerm === '' || option.text.toLowerCase().includes(searchTerm))) {
                const item = document.createElement('div');
                item.className = 'search-item';
                item.textContent = option.text;
                item.dataset.value = option.value;

                if (option.value === select.value) {
                    item.classList.add('selected');
                }

                resultsContainer.appendChild(item);
            }
        });
    }

    // Helper method for keyboard navigation in search results
    navigateSearchResults(resultsContainer, direction) {
        const items = resultsContainer.querySelectorAll('.search-item');
        if (items.length === 0) return;

        const selectedItem = resultsContainer.querySelector('.search-item.selected');
        let nextIndex = 0;

        if (selectedItem) {
            const currentIndex = Array.from(items).indexOf(selectedItem);
            selectedItem.classList.remove('selected');

            if (direction === 'down') {
                nextIndex = (currentIndex + 1) % items.length;
            } else {
                nextIndex = (currentIndex - 1 + items.length) % items.length;
            }
        }

        items[nextIndex].classList.add('selected');
        items[nextIndex].scrollIntoView({ block: 'nearest' });
    }

    toggleLoadingSpinner(isLoading) {
        if (isLoading) {
            // Show loading spinner
            this.pechaLoadingSpinner.classList.add('active');
            
            // Hide dropdown containers while loading
            this.searchContainers.forEach(container => {
                container.classList.add('loading');
            });
        } else {
            // Hide loading spinner
            this.pechaLoadingSpinner.classList.remove('active');
            
            // Show dropdown containers after loading
            this.searchContainers.forEach(container => {
                container.classList.remove('loading');
            });
        }
    }

    async toggleConditionalFields() {
        const isCommentaryOrTranslation = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);

        const isAlignment = this.annotationSelect?.value === 'alignment';

        if (!isCommentaryOrTranslation || !isAlignment) {
            this.pechaDropdown.value = "";
            this.parentAnnotation.value = "";
        }
        const fields = {
            'pechaField': isCommentaryOrTranslation && isAlignment,
            'parentAnnotationField': isAlignment && isCommentaryOrTranslation
        };

        Object.entries(fields).forEach(([fieldId, shouldShow]) => {
            document.getElementById(fieldId).style.display = shouldShow ? 'block' : 'none';
        });
        
        if(isCommentaryOrTranslation && isAlignment) {
            // Set the label dynamically based on relationship type
            if ('translation_of' in this.metadata && this.metadata.translation_of !== null) {
                this.pechaDropdownLabel.textContent = 'Translation of';
                const pechaId = this.metadata.translation_of;
                this.pechaDropdown.value = pechaId;
            } else if ('commentary_of' in this.metadata && this.metadata.commentary_of !== null) {
                this.pechaDropdownLabel.textContent = 'Commentary of';
                const pechaId = this.metadata.commentary_of;
                this.pechaDropdown.value = pechaId;
            }
            this.parentAnnotation.innerHTML = '<option value="">Select annotation</option>';
            this.parentAnnotation.remove(1)
            this.annotations.forEach(annotation => {
                const option = document.createElement('option');
                option.value = annotation.path;
                option.textContent = annotation.title;
                this.parentAnnotation.appendChild(option);
            });            
        }
        
        // Reset validation state
        this.form.classList.remove('was-validated');
    }


    async fetchMetadata(pechaId){
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
            console.log("meta ", metadata);
            return metadata;
        } catch (error) {
            console.error('Error fetching metadata:', error);
            this.showToast("Unable to fetch metadata", 'error');
            return null;
        }
    }

    async getAnnotation(pechaId) {
        const url = `${this.API_ENDPOINT}/annotation/${pechaId}`;
      
        try {
          const response = await fetch(url, {
            method: 'GET',
            headers: {
              'accept': 'application/json',
            },
          });
      
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
      
          const data = await response.json();
          console.log(data); // You can modify this to return data instead of logging
          return data;
        } catch (error) {
          console.error('Error fetching annotation:', error);
          throw error;
        }
    }

    extractAnnotations(data) {
        return Object.entries(data).map(([id, details]) => ({
            id: id,
            path: details.path,
            title: details.title,
            type: details.type || 'no type',
            aligned_to: details.aligned_to || null
        }));
    }
    
    // Display existing annotations for the selected pecha
    displayExistingAnnotations(annotations) {
        // Clear previous annotations
        this.existingAnnotationsList.innerHTML = '';
        
        // If there are no annotations or empty object, show a message
        if (!annotations || Object.keys(annotations).length === 0) {
            this.existingAnnotationsContainer.style.display = 'none';
            return;
        }
        
        // Show the container
        this.existingAnnotationsContainer.style.display = 'block';
        
        // Extract and format annotations for display
        const annotationItems = this.extractAnnotations(annotations);
        
        // Update the header to include the count of annotations
        const annotationsLabel = document.querySelector('.annotations-header label');
        if (annotationsLabel) {
            annotationsLabel.textContent = `Existing Annotations (${annotationItems.length})`;
        }
        
        // Create and append annotation items to the list
        if (annotationItems.length === 0) {
            const noAnnotationsElem = document.createElement('div');
            noAnnotationsElem.className = 'no-annotations';
            noAnnotationsElem.textContent = 'No annotations found for this pecha.';
            this.existingAnnotationsList.appendChild(noAnnotationsElem);
        } else {
            annotationItems.forEach((item, index) => {
                const annotationElem = document.createElement('div');
                annotationElem.className = 'annotation-item';
                annotationElem.dataset.id = item.id;
                annotationElem.dataset.path = item.path;
                annotationElem.dataset.title = item.title;
                annotationElem.dataset.type = item.type;
                if (item.aligned_to) {
                    annotationElem.dataset.alignedTo = JSON.stringify(item.aligned_to);
                }
                
                // Create a container for the title and index
                const titleContainer = document.createElement('div');
                titleContainer.className = 'title-container';
                
                // Add index badge
                const indexBadge = document.createElement('span');
                indexBadge.className = 'index-badge';
                indexBadge.textContent = index + 1;
                titleContainer.appendChild(indexBadge);
                
                // Add title
                const titleSpan = document.createElement('span');
                titleSpan.className = 'annotation-title';
                titleSpan.textContent = item.title;
                titleContainer.appendChild(titleSpan);
                
                // Add type
                const typeSpan = document.createElement('span');
                typeSpan.className = 'annotation-type';
                typeSpan.textContent = ` (${item.type})`;
                
                // Add edit button
                const editButton = document.createElement('button');
                editButton.className = 'edit-button';
                editButton.innerHTML = '<i class="fas fa-edit"></i>';
                editButton.type = 'button'; 
                editButton.onclick = (e) => {
                    e.preventDefault(); 
                    e.stopPropagation();
                    this.selectAnnotationForUpdate(item);
                };
                
                annotationElem.appendChild(titleContainer);
                annotationElem.appendChild(typeSpan);
                annotationElem.appendChild(editButton);
                
                this.existingAnnotationsList.appendChild(annotationElem);
            });
        }
        
        // Apply saved collapsed state if it exists
        const isCollapsed = localStorage.getItem('annotationsCollapsed') === 'true';
        const toggleBtn = document.getElementById('toggleAnnotationsBtn');
        const toggleIcon = document.getElementById('toggleIcon');
        
        if (isCollapsed) {
            this.existingAnnotationsList.classList.add('collapsed');
            if (toggleBtn) toggleBtn.classList.add('collapsed');
        } else {
            this.existingAnnotationsList.classList.remove('collapsed');
            if (toggleBtn) toggleBtn.classList.remove('collapsed');
        }
    }

    // New method to select an annotation for updating
    selectAnnotationForUpdate(annotation) {
        this.selectedAnnotation = annotation;
        
        // Pre-fill the form with the selected annotation's data
        this.annotationSelect.value = annotation.type;
        this.annotationTitle.value = annotation.title;
        
        // Update the submit button text
        const btnText = document.querySelector('#submitAnnotationBtn .btn-text');
        if (btnText) {
            btnText.textContent = 'Update Annotation';
        }
        
        // If it's an alignment annotation with a parent, set the parent annotation
        if (annotation.aligned_to && annotation.aligned_to.pecha_id) {
            this.pechaDropdown.value = annotation.aligned_to.pecha_id;
            
            if (annotation.aligned_to.alignment_id) {
                this.parentAnnotation.value = annotation.aligned_to.alignment_id;
            }
        }
        
        // Scroll to the form
        this.form.scrollIntoView({ behavior: 'smooth' });
        
        // Focus on the title field instead of showing a toast
        setTimeout(() => {
            this.annotationTitle.focus();
        }, 100);
    }

    async onPechaSelect(pechaId) {
        if (!pechaId) {
            // Hide annotations container if no pecha is selected
            this.existingAnnotationsContainer.style.display = 'none';
            this.existingAnnotationsList.innerHTML = '';
            return;
        }

        try {
            // Fetch metadata for the selected pecha
            this.metadata = await this.fetchMetadata(pechaId);
            console.log("metadata fetched : ", this.metadata);
            
            // Show annotations container and loading spinner
            this.existingAnnotationsContainer.style.display = 'block';
            this.existingAnnotationsList.innerHTML = '';
            this.annotationsLoadingSpinner.style.display = 'flex';
            
            // Fetch existing annotations for the selected pecha
            const existingAnnotations = await this.getAnnotation(pechaId);
            
            // Hide loading spinner
            this.annotationsLoadingSpinner.style.display = 'none';
            
            // Display existing annotations
            this.displayExistingAnnotations(existingAnnotations);
            
            // Handle commentary/translation relationship annotations
            const isCommentaryOrTranslation = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || 
                                          ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);
            const isAlignment = this.annotationSelect?.value === 'alignment';
            
            if (isCommentaryOrTranslation && isAlignment) {
                const parentPechaId = this.metadata.commentary_of ?? this.metadata.translation_of;
                const annotations = await this.getAnnotation(parentPechaId);
                this.annotations = this.extractAnnotations(annotations);
                console.log("annotation:", this.annotations);
            }
            
            this.toggleConditionalFields();
        } catch (error) {
            console.error('Error in onPechaSelect:', error);
            this.showToast('Failed to load pecha information', 'error');
        }
    }

    handleAnnotationChange(event) {
        this.toggleConditionalFields();
    }

    async fetchPechaList(filterBy) {
        let body = { filter: {} };

        try {
            // Show loading spinner
            this.toggleLoadingSpinner(true);
            
            let allPechas = [];
            let currentPage = 1;
            let hasMorePages = true;
            const limit = 100; // Keep the same limit per request
            
            // Loop until we've fetched all pages
            while (hasMorePages) {
                body.page = currentPage;
                body.limit = limit;
                
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
                allPechas = allPechas.concat(pechas.metadata);
                hasMorePages = pechas.metadata.length === limit;
                currentPage++;
            }
            
            // Hide loading spinner
            this.toggleLoadingSpinner(false);
            return allPechas;
        } catch (error) {
            // Hide loading spinner on error
            this.toggleLoadingSpinner(false);
            console.error("Error loading pecha options:", error);
            this.showToast("Unable to load pecha options. Please try again later.", 'error');
            return [];
        }
    }

    populatePechaDropdowns(pechas) {
        // Clear existing options first (keep only the default one)
        while (this.pechaSelect.options.length > 1) {
            this.pechaSelect.remove(1);
        }
        while (this.pechaDropdown.options.length > 1) {
            this.pechaDropdown.remove(1);
        }

        pechas.forEach(pecha => {
            const title = pecha.title[pecha.language] ?? pecha.title.bo;
            const option = new Option(`${pecha.id} - ${title}`, pecha.id);
            this.pechaSelect.add(option.cloneNode(true));
            this.pechaDropdown.add(option.cloneNode(true));
        });
    }

    async initializeForm() {
        try {
            // Show loading spinner before fetching pechas
            this.toggleLoadingSpinner(true);
            const pechas = await this.fetchPechaList();
            console.log("pecha:::", pechas);
            this.populatePechaDropdowns(pechas);
            // Loading spinner is already hidden in fetchPechaList
        } catch (error) {
            // Hide loading spinner in case of error
            this.toggleLoadingSpinner(false);
            console.error('Error initializing form:', error);
            this.showToast('Failed to initialize form. Please refresh the page.', 'error');
        }
    }

    getFormData() {
        // Create FormData from the form (this won't include disabled fields)
        const formData = new FormData(this.form);
        const data = Object.fromEntries(formData.entries());
        // Manually add values from disabled fields
        const disabledFields = this.form.querySelectorAll('input:disabled, select:disabled, textarea:disabled');
        disabledFields.forEach(field => {
            if (field.name && field.value) {
                data[field.name] = field.value;
            }
        });
        
        // Format the data according to the required structure
        const formattedData = {
            pecha_id: data.pecha,
            type: data.annotation_type,
            title: data.annotation_title
        };

        // Handle pecha_aligned_to based on whether it's a root pecha or not
        formattedData.aligned_to = data.pechaDropdown ? {
                pecha_id: data.pechaDropdown,
                alignment_id: data.parentAnnotation || null
            } : null;

        // If we're updating an existing annotation, include its ID/path
        if (this.selectedAnnotation) {
            formattedData.id = this.selectedAnnotation.id;
            formattedData.path = this.selectedAnnotation.path;
        }

        return formattedData;
    }

    async updateAnnotation(formData) {
        const response = await fetch(`${this.API_ENDPOINT}/annotation/${formData.id}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });

        if (!response.ok) {
            throw new Error('Failed to update annotation');
        }

        return response.json();
    }

    async handleSubmit(event) {
        event.preventDefault();
        // Disable submit button and show spinner inside
        const submitBtn = document.getElementById('submitAnnotationBtn');
        const btnText = submitBtn?.querySelector('.btn-text');
        const btnSpinner = submitBtn?.querySelector('.btn-spinner');
        if (submitBtn) submitBtn.disabled = true;
        if (btnText) btnText.style.display = 'none';
        if (btnSpinner) btnSpinner.style.display = 'inline-block';
        
        try {
            const data = this.getFormData();
            const isValid = this.validateForm(data);
            
            if (!isValid) {
                if (submitBtn) submitBtn.disabled = false;
                if (btnText) btnText.style.display = '';
                if (btnSpinner) btnSpinner.style.display = 'none';
                return;
            }
            
            if (!this.selectedAnnotation) {
                throw new Error('No annotation selected for update');
            }
            
            const response = await this.updateAnnotation(data);
            console.log("update response:", response);
            
            this.showToast('Annotation updated successfully!', 'success');
            
            // Refresh the annotations list
            this.onPechaSelect(this.pechaSelect.value);
            
            // Reset the form and selected annotation
            this.resetForm();
        } catch (error) {
            this.showToast(error.message, 'error');
            console.error('Error updating annotation:', error);
        } finally {
            if (submitBtn) submitBtn.disabled = false;
            if (btnText) btnText.style.display = '';
            if (btnSpinner) btnSpinner.style.display = 'none';
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

    validateForm(data) {
        if (!data.pecha_id) {
            this.highlightError(this.pechaSelect);
            this.showToast('Pecha is required', 'error');
            return false;
        }
        
        const isCommentaryOrTranslation = ('translation_of' in this.metadata && this.metadata.translation_of !== null) || ('commentary_of' in this.metadata && this.metadata.commentary_of !== null);
        const isAlignment = this.annotationSelect?.value === 'alignment';

        if (isCommentaryOrTranslation && isAlignment) {
            if (!data.aligned_to.alignment_id) {
                this.highlightError(this.parentAnnotation);
                this.showToast('Alignment annotation is required', 'error');
                return false;
            }
        }
        
        if (!data.title) {
            this.highlightError(this.annotationTitle);
            this.showToast('Annotation Title is required', 'error');
            return false;
        }
        
        // Check for duplicate annotation titles (excluding the current one being edited)
        const existingItems = this.existingAnnotationsList.querySelectorAll('.annotation-item');
        for (let i = 0; i < existingItems.length; i++) {
            const item = existingItems[i];
            const titleElement = item.querySelector('.annotation-title');
            
            // Skip checking the current annotation being edited
            if (this.selectedAnnotation && item.dataset.id === this.selectedAnnotation.id) {
                continue;
            }
            
            if (titleElement && titleElement.textContent.toLowerCase() === data.title.toLowerCase()) {
                this.highlightError(this.annotationTitle);
                this.showToast('An annotation with this title already exists for this pecha. Please use a different title.', 'error');
                return false;
            }
        }
        
        return true;
    }

    resetForm() {
        this.form.reset();
        this.selectedAnnotation = null;
        this.toggleConditionalFields();
        
        // Reset the submit button text
        const btnText = document.querySelector('#submitAnnotationBtn .btn-text');
        if (btnText) {
            btnText.textContent = 'Update Annotation';
        }
    }
}

// Initialize the form when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new AnnotationForm();
});