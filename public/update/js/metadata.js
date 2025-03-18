class MetadataManager {
    constructor() {
        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.elements = {
            pechaOptionsContainer: document.getElementById('pechaOptionsContainer'),
            toastContainer: document.getElementById('toastContainer'),
            formContent: document.getElementById('formContent'),
            metadataForm: document.getElementById('metadata-form'),
            formGroups: document.querySelectorAll('.form-group'),
            documentField: document.getElementById('document'),
            language: document.getElementById('language'),
            source: document.getElementById('source-field'),
            altTitles: document.getElementById('alt-titles'),
            addAltTitle: document.getElementById('addAltTitle'),
            relationRadios: document.querySelectorAll('input[name="relation"]'),
            relatedPechaContainer: document.getElementById('relatedPechaContainer'),
            updateButton: document.querySelector('.create-button'),
            buttonText: document.querySelector('.create-button span'),
            spinner: document.querySelector('.spinner'),
            toastContainer: document.getElementById('toastContainer'),
            updateFormContainer: document.getElementById('updateFormContainer')
        };
        this.languageOptions = [];
        this.selectedPechaId = null;

        this.initialize();
    }

    async initialize() {
        await this.fetchPechaOptions();
        this.fetchLanguages();
        this.setupEventListeners();
    }

    setupEventListeners() {
       // action handler for pecha selection
        this.elements.pechaOptionsContainer.addEventListener('customDropdownChange', (e) => {
            this.handlePechaSelection(e.detail.value);
        });

        // Type Radio Selection
        this.elements.relationRadios.forEach((radio) => {
            radio.addEventListener("change", () => {
                this.fetchRelatedPechaOptions(radio.value);
            });
        });

        // Add Localization Buttons
        document.querySelectorAll(".add-localization").forEach((button) => {
            button.addEventListener("click", (e) => {
                const formGroup = e.target.closest(".form-group");
                this.addLocalization(formGroup);
            });
        });

        // Add Alternate Title Button
        this.elements.addAltTitle.addEventListener("click", () => {
            this.addAltTitles();
        });

        // Submit form
        document.getElementById('metadata-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleUpdateMetadata();
        });
    }

    async fetchLanguages() {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/languages`, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            this.languageOptions = data;
            this.updateLanguageOptions();
            return data;
        } catch (error) {
            console.error('Error fetching languages:', error);
            this.showToast('Error fetching languages: ' + error.message, 'error');
        }
    }

    updateLanguageOptions() {
        this.elements.language.innerHTML = '<option value="">Select language</option>';
        this.languageOptions.forEach(lang => {
            const option = document.createElement('option');
            option.value = lang.code;
            option.textContent = lang.name;
            this.elements.language.appendChild(option);
        });
    }

    async fetchPechaOptions() {
        try {
            this.showSpinner(this.elements.pechaOptionsContainer, true);
            this.elements.formContent.style.display = 'none';
            const response = await fetch(`${this.API_ENDPOINT}/metadata/filter/`, {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({})
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const pechas = await response.json();
            this.updatePechaOptions(pechas);
        } catch (error) {
            console.error('Error loading pecha options:', error);
            this.showToast('Error loading pecha options: ' + error.message, 'error');
        } finally {

            this.showSpinner(this.elements.pechaOptionsContainer, false);
            this.elements.formContent.style.display = 'block';
        }
    }

    updatePechaOptions(pechas) {
        this.elements.pechaOptionsContainer.innerHTML = '';
        new CustomSearchableDropdown(
            this.elements.pechaOptionsContainer, 
            pechas, 
            "selectedPecha", 
            (pechaId) => this.handlePechaSelection(pechaId)
        );
    }

    async fetchRelatedPechaOptions(relationshipType, pechaId) {
        if (!this.elements.relatedPechaContainer) return;

        let body = { filter: {} };
        const filters = {
            "version_of": {
                "and": [
                    { "field": "commentary_of", "operator": "==", "value": null },
                    { "field": "version_of", "operator": "==", "value": null } 
                ]
            },
            "commentary_of": {
                "and": [
                    { "field": "commentary_of", "operator": "==", "value": null },
                    { "field": "translation_of", "operator": "==", "value": null }
                ]
            },
            "translation_of": {
                "field": "language",
                "operator": "==",
                "value": "bo"
            }
        };

        body.filter = filters[relationshipType] || {};

        try {
            this.showSpinner(this.elements.relatedPechaContainer, true);
            
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
            this.updateRelatedPechaOptions(pechas,pechaId);
        } catch (error) {
            console.error("Error loading related pecha options:", error);
            this.showToast("Unable to load related pecha options: " + error.message, "error");
        } finally {
            this.showSpinner(this.elements.relatedPechaContainer, false);
        }
    }

    updateRelatedPechaOptions(pechas,pechaId) {
        this.elements.relatedPechaContainer.innerHTML = '';
        new CustomSearchableDropdown(
            this.elements.relatedPechaContainer, 
            pechas, 
            "relatedPecha"
        );

        const customDropdown = document.getElementById("relatedPecha");
        const pecha = pechas.find(p => p.id === pechaId);
        customDropdown.dataset.value = pecha?.id || '';
        customDropdown.textContent = pecha ? `${pecha.id} - ${pecha.title}` : 'Select pecha';
    }

    handlePechaSelection(pechaId) {
        if (!pechaId) return;
        
        this.selectedPechaId = pechaId;
        console.log("Selected pecha ID:", pechaId);
        this.enableForm();
        this.loadPechaMetadata(pechaId);
    }

    async loadPechaMetadata(pechaId) {
        this.setUpdatingState(true);
        
        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${pechaId}`);
            
            if (!response.ok) {
                throw new Error('Failed to fetch metadata');
            }
            
            const metadata = await response.json();
            console.log("Loaded metadata:", metadata);
            this.populateForm(metadata);
        } catch (error) {
            console.error("Error loading metadata:", error);
            this.showToast('Error loading metadata: ' + error.message, 'error');
        } finally {
            this.setUpdatingState(false);
        }
    }

    populateForm(metadata) {
        // Clear existing form data
        this.elements.metadataForm.reset();
        this.clearLocalizations();
        
        // Populate simple fields
        this.elements.documentField.value = metadata.document_id || '';
        this.elements.source.value = metadata.source == "None" ? '' : metadata.source;
        this.elements.language.value = this.languageOptions.find(lang => lang.code === metadata.language)?.code || '';
        // Determine relationship type if available
        if (metadata.version_of) {
            this.elements.relationRadios[0].checked = true;
            this.fetchRelatedPechaOptions('version_of', metadata.version_of);
            // We would need to set the selected related pecha here
        } else if (metadata.commentary_of) {
            this.elements.relationRadios[1].checked = true;
            this.fetchRelatedPechaOptions('commentary_of', metadata.commentary_of);
        } else if (metadata.translation_of) {
            this.elements.relationRadios[2].checked = true;
            this.fetchRelatedPechaOptions('translation_of', metadata.translation_of);
        }
        
        // Populate localized fields
        this.populateLocalizedField('presentation', metadata.presentation);
        this.populateLocalizedField('title', metadata.title);
        this.populateLocalizedField('long_title', metadata.long_title);
        this.populateLocalizedField('author', metadata.author);
        this.populateLocalizedField('usage_title', metadata.usage_title);
        this.populateAlternateTitles(metadata.alt_titles);
    }

    populateAlternateTitles(data) {
        this.elements.altTitles.innerHTML = '';
        if (!data || data.length === 0) return;
        this.addAltTitles(data);
    }

    clearLocalizations() {
        document.querySelectorAll('.form-group[data-field] .localizations').forEach(container => {
            container.innerHTML = '';
        });
    }

    populateLocalizedField(fieldName, data) {
        if (!data || Object.keys(data).length === 0) return;
        
        const container = document.querySelector(`.form-group[data-field="${fieldName}"] .localizations`);
        if (!container) return;
        
        container.innerHTML = '';
        
        Object.entries(data).forEach(([lang, value], index) => {
            this.createLocalizationInput(container, lang, value, index === 0);
        });
    }

    createLocalizationInput(container, lang = '', value = '', isFirst = false) {
        const inputContainer = document.createElement("div");
        inputContainer.className = "input-container";

        const inputGroup = document.createElement("div");
        inputGroup.className = "input-group";

        const inputWrapper = document.createElement("div");
        inputWrapper.className = "input-wrapper";

        const isTextarea = container.closest(".form-group")?.dataset.field === "presentation";
        let input;

        if (isTextarea) {
            input = document.createElement("textarea");
            input.placeholder = "Enter text";
            input.value = value;
        } else {
            input = document.createElement("input");
            input.type = "text";
            input.placeholder = "Enter text";
            input.value = value;
        }

        // input.setAttribute("required", "");
        const langSelect = document.createElement("select");
        let temp = `<option value="">Language</option>`;
        this.languageOptions.forEach(option => {
            temp += `<option value=${option.code} ${option.code == lang ? 'selected' : ''}>${option.name}</option>`;
        });
        langSelect.innerHTML = temp;
        // langSelect.setAttribute("required", "");

        inputWrapper.appendChild(input);
        inputWrapper.appendChild(langSelect);
        inputGroup.appendChild(inputWrapper);

        if (!isFirst) {
            const removeButton = document.createElement("button");
            removeButton.type = "button";
            removeButton.className = "remove-btn";
            removeButton.title = "remove"
            removeButton.innerHTML = '<i class="fa fa-minus" aria-hidden="true"></i>';
            removeButton.addEventListener("click", () => {
                inputContainer.remove();
            });
            inputGroup.appendChild(removeButton);
        }

        inputContainer.appendChild(inputGroup);
        container.appendChild(inputContainer);
        return inputContainer;
    }

    addLocalization(formGroup) {
        const localizationsDiv = formGroup.querySelector('.localizations');
        const inputs = localizationsDiv.querySelectorAll('input, textarea');
        const selects = localizationsDiv.querySelectorAll('select');
        
        // Check if all existing inputs are filled
        let allFilled = true;
        inputs.forEach((input, index) => {
            if (!input.value || !selects[index].value) {
                allFilled = false;
                input.closest(".input-wrapper").classList.add("error");
            }else{
                input.closest(".input-wrapper").classList.remove("error");
            }
        });
        
        if (!allFilled) {
            this.showToast('Please fill in all existing fields before adding a new field', 'warning');
            return;
        }
        
        // Clear any existing error states
        inputs.forEach(input => {
            input.classList.remove('error');
        });
        
        // Add new localization input
        this.createLocalizationInput(localizationsDiv);
    }

    addAltTitles(alternateTitles = []) {
        if(!this.areAllFieldsFilled()) {
            this.showToast('Please fill in all existing fields before adding a new alternate title', 'warning');
            return;
        }
        // Create a new alt-title group
        const altTitleGroup = document.createElement("div");
        altTitleGroup.className = "form-group alt-title-group";

        // Create localizations container
        const localizationsDiv = document.createElement("div");
        localizationsDiv.className = "localizations";

        // Add the first localization input
        if (alternateTitles && alternateTitles.length > 0) {
            console.log("yes")
            alternateTitles.forEach(title => {
                const [[key, value]] = Object.entries(title);
                this.createLocalizationInput(localizationsDiv, key, value, false);
            });
        }else{
            console.log("no")
            this.createLocalizationInput(localizationsDiv);
        }

        // Create add localization button
        const addLocalizationBtn = document.createElement("button");
        addLocalizationBtn.type = "button";
        addLocalizationBtn.className = "add-localization";
        addLocalizationBtn.innerHTML = '<i class="fas fa-plus-circle"></i> Add Title';
        addLocalizationBtn.addEventListener("click", (e) => {
            const formGroup = e.target.closest(".form-group");
            this.addLocalization(formGroup);

        });

        // Create remove alt-title button
        const removeAltTitleBtn = document.createElement("button");
        removeAltTitleBtn.type = "button";
        removeAltTitleBtn.className = "remove-alt-title";
        removeAltTitleBtn.title = "delete"
        removeAltTitleBtn.innerHTML = '<i class="fas fa-times" aria-hidden="true"></i>';
        removeAltTitleBtn.addEventListener("click", () => {
            altTitleGroup.remove();
        });

        // Assemble the alt-title group
        altTitleGroup.appendChild(localizationsDiv);
        altTitleGroup.appendChild(addLocalizationBtn);
        altTitleGroup.appendChild(removeAltTitleBtn);

        // Add to the form
        this.elements.altTitles.appendChild(altTitleGroup);
    }

    areAllFieldsFilled() {
        const altTitleGroups = document.querySelectorAll('.alt-title-group');
        let allFilled = true;
    
        altTitleGroups.forEach(group => {
            const inputs = group.querySelectorAll('input[type="text"]');
            const selects = group.querySelectorAll('select');
    
            inputs.forEach((input, index) => {
                if (!input.value.trim() || !selects[index].value) {
                    allFilled = false;
                    input.closest('.input-wrapper').classList.add('error');
                } else {
                    input.closest('.input-wrapper').classList.remove('error');
                }
            });
        });
    
        return allFilled;
    }

    collectFormData() {
        const document_id = this.extractDocIdFromLink(this.elements.documentField.value);
        const metadata = {
            document_id: document_id,
            language: this.elements.language.value.trim(),
            source: this.elements.source.value.trim()
        };
        
        // Collect localized fields
        document.querySelectorAll('.form-group[data-field]').forEach(group => {
            const fieldName = group.dataset.field;
            const localizations = {};
            group.querySelectorAll('.input-container').forEach(item => {
                const langCode = item.querySelector('select')?.value.trim();
                const langValue = item.querySelector('input,textarea')?.value.trim();
                
                if (langCode && langValue) {
                    localizations[langCode] = langValue;
                }
            });
            
            if (Object.keys(localizations).length > 0) {
                metadata[fieldName] = localizations;
            }
        });
        
        // collect alternate titles
        const alt_titles = [];
        const altTitleGroups = document.querySelectorAll('.alt-title-group');

        altTitleGroups.forEach(group => {
            const alternateTitle = {};

            // Select all input fields and their corresponding select fields within the group
            const inputs = group.querySelectorAll('input');
            console.log("inputs ", inputs);
            const selects = group.querySelectorAll('select');

            // Iterate over inputs and selects and map them together based on their index
            inputs.forEach((input, index) => {
                const language = selects[index].value;
                // if (!language) {
                //     this.showToast("Please select a language for each alternate title", "warning");
                //     return;
                // }
                if (language && input.value.trim()) {
                    alternateTitle[language] = input.value.trim(); // Add to the object if both language and value exist
                }
            });

            // Push the alternateTitle object to the alt_titles array if it has any data
            if (Object.keys(alternateTitle).length > 0) {
                alt_titles.push(alternateTitle);
            }
        });

        if (alt_titles.length > 0)
            metadata.alt_titles = alt_titles;

        // Collect document type and related pecha
        const relationType = document.querySelector('input[name="relation"]:checked');
        if (relationType) {
            const customDropdown = document.getElementById("relatedPecha");
            const relatedPechaId = customDropdown?.dataset.value;
            
            if (relatedPechaId) {
                metadata[relationType.value] = relatedPechaId;
            }   
        }
        
        return metadata;
    }

    validateForm(formData) {
        const required = ['author', 'document_id', 'title', 'long_title', 'language', 'source'];
        const missing = required.filter(field => !formData[field]);
    
        // Check for missing localizations in title
        const titleLocalizations = formData.title || {};
        const hasTibetan = titleLocalizations['bo'] && titleLocalizations['bo'].trim() !== '';
        const hasEnglish = titleLocalizations['en'] && titleLocalizations['en'].trim() !== '';
        const hasLanguage = titleLocalizations[formData.language] && titleLocalizations[formData.language].trim() !== '';
    
        if (!hasTibetan || !hasEnglish || !hasLanguage) {
            missing.push(`title (must include Tibetan, English, and ${this.languageOptions.find(lang => lang.code === formData.language)?.name})`);
        }
    
        // Check if any required fields are missing
        if (missing.length > 0) {
            this.showToast(`Missing required fields: ${missing.join(', ')}`, 'error');
    
            // Highlight missing fields
            missing.forEach(field => {
                if (field === 'document_id' || field === 'language' || field === 'author' || field === 'source') {
                    document.getElementById(field).classList.add('error');
                } else {
                    const container = document.querySelector(`.form-group[data-field="${field}"]`);
                    if (container) {
                        container.classList.add('error');
                    }
                }
            });
    
            return false;
        }
    
        // Ensure no fields are left incomplete for localizations
        const localizationFields = [formData.title, formData.long_title, formData.author];
        for (const field of localizationFields) {
            if (typeof field === 'object') {
                for (const lang in field) {
                    if (!field[lang] || field[lang].trim() === '') {
                        this.showToast(`Please fill in all localizations for ${field}`, 'error');
                        return false;
                    }
                }
            }
        }
    
        return true;
    }
    
    clearForm() {
        this.elements.metadataForm.reset();
        this.clearLocalizations();
        this.clearAltTitles();
    }
    
    disableForm() {
        this.elements.formContent.classList.add('disabled');
    }

    enableForm() {
        this.elements.formContent.classList.remove('disabled');
    }

    isValidUrl(string) {
        try {
            new URL(string);
            return true;
        } catch (_) {
            return false;
        }
    }

    extractDocIdFromLink(docLink) {
        console.log("docLink", docLink)
        // Typical Docs link: https://docs.google.com/document/d/<DOC_ID>/edit
        // We want the <DOC_ID> after "/d/" and before "/edit"
        const regex = /\/d\/([^/]+)/;
        const match = docLink.match(regex);
        return match ? match[1] : docLink;
    }

    clearErrors() {
        document.querySelectorAll('.error').forEach(element => {
            element.classList.remove('error');
        });
    }

    async handleUpdateMetadata() {
        if (!this.selectedPechaId) {
            this.showToast('Please select a Pecha', 'error');
            return;
        }

        this.clearErrors();
        const formData = this.collectFormData();
        
        if (!this.validateForm(formData)) {
            return;
        }
        this.setUpdatingState(true);
        
        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${this.selectedPechaId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ metadata: formData })
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || 'Failed to update metadata');
            }

            this.showToast('Metadata updated successfully!', 'success');
            // this.clearForm();
        } catch (error) {
            console.error('Error updating metadata:', error);
            this.showToast(error.message, 'error');
        } finally {
            this.setUpdatingState(false);
        }
    }

    setUpdatingState(isUpdating) {
        this.updating = isUpdating;
        this.elements.updateButton.disabled = isUpdating;
        this.elements.buttonText.textContent = isUpdating ? 'Updating...' : 'Update';
        this.elements.spinner.style.display = isUpdating ? 'inline-block' : 'none';
        if (isUpdating) {
            this.disableForm();
        } else {
            this.enableForm();
        }
    }

    showLoading(isLoading) {
        const loader = document.getElementById('loading-indicator');
        if (loader) {
            loader.style.display = isLoading ? 'block' : 'none';
        }
    }

    showSpinner(parentNode, show) {
        if (show) {
            parentNode.innerHTML = '';
            const spinner = document.createElement('div');
            spinner.className = 'spinner';
            spinner.style.display = 'inline-block';
            parentNode.appendChild(spinner);
        } else {
            const spinner = parentNode.querySelector('.spinner');
            if (spinner) {
                spinner.remove();
            }
        }
    }

    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `${this.getToastIcon(type)} ${message}`;
        this.elements.toastContainer.appendChild(toast);

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
}

document.addEventListener('DOMContentLoaded', () => {
    new MetadataManager();
});