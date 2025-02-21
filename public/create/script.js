class LocalizedForm {
    constructor() {
        this.API_ENDPOINT = 'https://api-aq25662yyq-uc.a.run.app';
        this.baseLanguageSelect = document.getElementById("baseLanguage");
        this.baseLanguageLoader = document.getElementById("baseLanguageLoader");
        this.formContent = document.getElementById("formContent");
        this.formGroups = document.querySelectorAll(".form-group");
        this.addAltTitleButton = document.getElementById("addAltTitle");
        this.pechaOptionsContainer = document.getElementById(
            "pechaOptionsContainer"
        );
        this.pechaOptions = document.getElementById("pechaOptions");
        this.typeRadios = document.querySelectorAll(
            'input[name="documentType"]'
        );
        this.createButton = document.getElementById("createButton")
        this.createBtnText = document.querySelector(".create-button-text");
        this.creatingSpinner = createButton.querySelector(".spinner")
        this.creating = false;
        this.setupEventListeners();
        this.languageOptions = [];
        this.fetchLanguages().then(languages => {
            this.languageOptions = languages;
            let temp = `<option value="">Language</option>`;
            languages.forEach(lang => {
                temp += `<option value="${lang.code}">${lang.name}</option>`;
            });

            this.baseLanguageSelect.innerHTML = temp;
        });
    }

    setupEventListeners() {
        // Base Language Selection
        this.baseLanguageSelect.addEventListener("change", () => {
            const baseLanguage = this.baseLanguageSelect.value;
            if (baseLanguage) {
                this.formContent.classList.add("visible");
                this.initializeFields(baseLanguage);
            } else {
                this.formContent.classList.remove("visible");
            }
            // will remove the alternate titles if the base language is changed
            const altTitles = document.getElementById("alt-titles");
            altTitles.innerHTML = "";
        });
        // Add Localization Buttons
        document.querySelectorAll(".add-localization").forEach((button) => {
            button.addEventListener("click", (e) => {
                const formGroup = e.target.closest(".form-group");
                // this.addLocalization(formGroup);
                if (formGroup.classList.contains('alt-title-group')) {
                    this.addLocalizationToAltTitle(formGroup);
                } else {
                    this.addLocalization(formGroup);
                }
            });
        });
        // Add Alternative Title Button
        this.addAltTitleButton.addEventListener("click", () => {
            const baseLanguage = this.baseLanguageSelect.value;
            this.addAltTitles(baseLanguage);
        });
        // Type Radio Selection
        this.typeRadios.forEach((radio) => {
            radio.addEventListener("change", () => {
                console.log("value:::", radio.value);
                if (radio.checked) {
                    this.pechaOptionsContainer.classList.add("visible");
                    this.fetchPechaOptions(radio.value);
                }
            });
        });
        // Publish Button
        this.createButton.addEventListener("click", () => {
            this.handleCreatePecha();
        });
    }

    initializeFields(baseLanguage) {
        document
            .querySelectorAll(".form-group[data-field]")
            .forEach((group) => {
                const localizationsDiv = group.querySelector(".localizations");
                localizationsDiv.innerHTML = ""; // Clear existing
                this.createLocalizationInput(
                    localizationsDiv,
                    baseLanguage,
                    true
                );
            });


    }

    setCreatingState(creating) {
        this.creating = creating;
        this.createButton.disabled = creating;
        this.createBtnText.textContent = creating ? 'Creating...' : 'Create';
        this.creatingSpinner.style.display = creating ? 'inline-block' : 'none';

        this.formGroups.forEach(group => {
            group.classList.toggle('disabled', creating);
        });
        this.baseLanguageSelect.disabled = creating;
    }

    createLocalizationInput(container, language, isFirst = false) {
        const inputContainer = document.createElement("div");
        inputContainer.className = "input-container";

        const inputGroup = document.createElement("div");
        inputGroup.className = "input-group";

        const inputWrapper = document.createElement("div");
        inputWrapper.className = "input-wrapper";

        const isTextarea =
            container.closest(".form-group")?.dataset.field === "presentation";
        let input;

        if (isTextarea) {
            input = document.createElement("textarea");
            input.placeholder = "Enter text";
        } else {
            input = document.createElement("input");
            input.type = "text";
            input.placeholder = "Enter text";
        }

        input.setAttribute("required", "");
        const langSelect = document.createElement("select");
        let temp = `<option value="">Language</option>`;
        this.languageOptions.forEach(lang => {
            temp += `<option value="${lang.code}">${lang.name}</option>`;
        });
        langSelect.innerHTML = temp;
        langSelect.setAttribute("required", "");

        if (isFirst) {
            langSelect.value = language;
            langSelect.disabled = true;
        }

        inputWrapper.appendChild(input);
        inputWrapper.appendChild(langSelect);
        inputGroup.appendChild(inputWrapper);

        if (!isFirst) {
            const removeButton = document.createElement("button");
            removeButton.type = "button";
            removeButton.className = "remove-btn";
            removeButton.innerHTML = '<i class="fas fa-times"></i>';
            removeButton.addEventListener("click", () => {
                inputContainer.remove();
            });
            inputGroup.appendChild(removeButton);
        }

        inputContainer.appendChild(inputGroup);
        container.appendChild(inputContainer);
    }

    addLocalization(formGroup) {
        const localizationsDiv = formGroup.querySelector(".localizations");
        const inputs = localizationsDiv.querySelectorAll("input, textarea");
        const selects = localizationsDiv.querySelectorAll("select");

        // Validate existing fields
        let isValid = true;
        inputs.forEach((input, index) => {
            if (!input.value || !selects[index].value) {
                isValid = false;
                input.closest(".input-wrapper").classList.add("error");
            }
        });

        if (!isValid) {
            // alert(
            //     "Please fill in all existing fields before adding a new localization"
            // );
            this.showToast("Please fill in all existing fields before adding a new localization", "warning");
            return;
        }

        // Clear any existing error states
        localizationsDiv
            .querySelectorAll(".input-wrapper")
            .forEach((wrapper) => {
                wrapper.classList.remove("error");
            });

        this.createLocalizationInput(localizationsDiv);
    }

    addLocalizationToAltTitle(altTitleGroup) {
        const localizationsDiv = altTitleGroup.querySelector(".localizations");
        const inputs = localizationsDiv.querySelectorAll("input");
        const selects = localizationsDiv.querySelectorAll("select");

        // Validate existing fields
        let isValid = true;
        inputs.forEach((input, index) => {
            if (!input.value || !selects[index].value) {
                isValid = false;
                input.closest(".input-wrapper").classList.add("error");
            }
        });

        if (!isValid) {
            // alert("Please fill in all existing fields before adding a new localization");
            this.showToast("Please fill in all existing fields before adding a new localization", "warning");
            return;
        }

        // Clear any existing error states
        localizationsDiv.querySelectorAll(".input-wrapper").forEach((wrapper) => {
            wrapper.classList.remove("error");
        });

        this.createLocalizationInput(localizationsDiv);
    }

    addAltTitles(baseLanguage) {
        const altTitles = document.getElementById("alt-titles");

        // Create a new alt-title group
        const altTitleGroup = document.createElement("div");
        altTitleGroup.className = "form-group alt-title-group";

        // Create localizations container
        const localizationsDiv = document.createElement("div");
        localizationsDiv.className = "localizations";

        // Add the first localization input
        this.createLocalizationInput(localizationsDiv, baseLanguage, true);

        // Create add localization button
        const addLocalizationBtn = document.createElement("button");
        addLocalizationBtn.type = "button";
        addLocalizationBtn.className = "add-localization";
        addLocalizationBtn.innerHTML = '<i class="fas fa-plus-circle"></i> Add Localization';
        addLocalizationBtn.addEventListener("click", (e) => {
            const formGroup = e.target.closest(".form-group");
            this.addLocalization(formGroup);

        });

        // Create remove alt-title button
        const removeAltTitleBtn = document.createElement("button");
        removeAltTitleBtn.type = "button";
        removeAltTitleBtn.className = "remove-alt-title";
        removeAltTitleBtn.innerHTML = '<i class="fas fa-times"></i>';
        removeAltTitleBtn.addEventListener("click", () => {
            altTitleGroup.remove();
        });

        // Assemble the alt-title group
        altTitleGroup.appendChild(localizationsDiv);
        altTitleGroup.appendChild(addLocalizationBtn);
        altTitleGroup.appendChild(removeAltTitleBtn);

        // Add to the form
        altTitles.appendChild(altTitleGroup);
    }

    async fetchPechaOptions(filterBy) {
        let body = { filter: {} };

        if (filterBy === "commentary_of") {
            body.filter = { "field": "commentary_of", "operator": "!=", "value": null };
        } else if (filterBy === "version_of") {
            body.filter = { "field": "version_of", "operator": "!=", "value": null };
        } else if (filterBy === "translation_of") {
            body.filter = { "field": "translation_of", "operator": "!=", "value": null };
        }

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
            console.log("Pecha options:", pechas);

            // Clear existing options, keep the first "Select pecha" option
            while (this.pechaOptions.options.length > 1) {
                this.pechaOptions.remove(1);
            }

            pechas.forEach((pecha) => {
                const option = document.createElement("option");
                option.value = pecha.id;
                option.textContent = `(${pecha.id}) ${pecha.title}`;
                this.pechaOptions.appendChild(option);
            });
            console.log("Dropdowns populated successfully.");
        } catch (error) {
            console.error("Error loading pecha options:", error);
            alert("Unable to load pecha options. Please try again later.");
        }
    }

    async fetchLanguages() {
        this.baseLanguageSelect.style.display = "none";
        this.baseLanguageLoader.style.display = "inline-block";
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
            // console.log("languages ::",data);
            return data;
        } catch (error) {
            console.error('Error fetching languages:', error);
        } finally {
            this.baseLanguageSelect.style.display = "inline-block";
            this.baseLanguageLoader.style.display = "none";
        }
    }

    collectFormData() {
        const metadata = {
            language: this.baseLanguageSelect.value,
        }
        // Collect localized fields
        document.querySelectorAll(".form-group[data-field]").forEach(group => {
            const fieldName = group.dataset.field;
            const localizations = {};

            group.querySelectorAll(".input-container").forEach(container => {
                const input = container.querySelector("input, textarea");
                const select = container.querySelector("select");

                if (input && input.value && select) {
                    const lang = select.disabled ? this.baseLanguageSelect.value : select.value;
                    localizations[lang] = input.value;
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
        // Collect non-localized fields
        const selectedDate = document.getElementById("selectedDate").innerHTML;
        if (selectedDate && selectedDate !== "No date selected") {
            metadata.date = selectedDate;
        }
        metadata.source = document.querySelector('input[placeholder="https://example.com"]').value;

        // Collect document type and pecha
        const selectedType = document.querySelector('input[name="documentType"]:checked');
        if (selectedType) {
            metadata[selectedType.value] = this.pechaOptions.value
        }
        // Collect Google Docs id 
        metadata.document_id = this.extractDocIdFromLink(document.querySelector('input[placeholder="Google docs URL"]').value);

        return metadata
    }

    validateRequiredFields(metadata) {
        const errors = [];

        // Check author in English
        if (!metadata.author) {
            errors.push('author');
            this.highlightError('author');
        }

        // Check source URL
        if (!metadata.source) {
            errors.push('source');
            document.querySelector('input[placeholder="https://example.com"]')
                .closest('.input-wrapper').classList.add('error');
        }

        // Check title in English
        if (!metadata.title) {
            errors.push('title');
            this.highlightError('title');
        }

        // Check long title in English
        if (!metadata.long_title) {
            errors.push('long_title');
            this.highlightError('long_title');
        }

        // Check base language
        if (!metadata.language) {
            errors.push('language');
            this.baseLanguageSelect.classList.add('error');
        }

        if (!metadata.document_id) {
            errors.push('document_id');
            document.querySelector('input[placeholder="Google docs URL"]')
                .closest('.input-wrapper').classList.add('error');
        }
        return errors.length === 0;
    }

    highlightError(fieldName) {
        const formGroup = document.querySelector(`.form-group[data-field="${fieldName}"]`);
        if (formGroup) {
            const inputs = formGroup.querySelectorAll('.input-container');
            inputs.forEach(container => {
                container.querySelector('.input-wrapper').classList.add('error');

            });
        }
    }

    clearErrors() {
        // Clear all error states
        document.querySelectorAll('.error').forEach(element => {
            element.classList.remove('error');
        });
        this.baseLanguageSelect.classList.remove('error');
    }

    extractDocIdFromLink(docLink) {
        console.log("doc link ", docLink);
        // Typical Docs link: https://docs.google.com/document/d/<DOC_ID>/edit
        // We want the <DOC_ID> after "/d/" and before "/edit"
        const regex = /\/d\/([^/]+)/;
        const match = docLink.match(regex);
        return match ? match[1] : null;
    }

    async handleCreatePecha() {
        try {
            this.clearErrors();
            const metadata = this.collectFormData();

            if (!this.validateRequiredFields(metadata)) {
                throw new Error('Please fill in all required fields.');
            }

            if (!metadata.document_id) {
                throw new Error('Invalid Google Docs link');
            }

            this.setCreatingState(true);
            // Fetch document and prepare form data concurrently
            const [blob] = await Promise.all([
                downloadDoc(metadata.document_id).catch(err => {
                    throw new Error(`Download failed: ${err.message}`);
                })
            ]);

            const formData = await this.prepareFormData(blob, metadata);

            const response = await this.submitFormData(formData);

            if (!response.ok) {
                const error = await response.text();
                throw new Error(`Failed to create: ${error}`);
            }

            const jsonResponse = await response.json();
            await this.handleSuccessfulSubmission(jsonResponse);

            this.showToast("File and metadata successfully submitted!", "success");
            this.clearForm();

        } catch (error) {
            console.error('Error in handleCreatePecha:', error);
            this.showToast(error, "error");
        } finally {
            this.setCreatingState(false);
        }
    }

    // Helper methods to publish
    async prepareFormData(blob, metadata) {
        const formData = new FormData();
        formData.append("text", blob, `text_${metadata.document_id}.docx`);
        formData.append("metadata", JSON.stringify(metadata));
        return formData;
    }

    async submitFormData(formData) {
        return fetch(`${this.API_ENDPOINT}/pecha/`, {
            method: "POST",
            body: formData
        });
    }

    async handleSuccessfulSubmission(jsonResponse) {
        const { pecha_id, data: serializedData } = jsonResponse;

        const blob = new Blob([JSON.stringify(serializedData, null)], {
            type: "application/json"
        });

        await this.downloadFile(blob, `${pecha_id}.json`);
    }

    async downloadFile(blob, filename) {
        const downloadUrl = URL.createObjectURL(blob);
        try {
            const a = document.createElement("a");
            a.href = downloadUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
        } finally {
            URL.revokeObjectURL(downloadUrl);
        }
    }

    showToast(message, type) {
        const toastContainer = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        toastContainer.appendChild(toast);

        // Auto-close the toast after 3 seconds
        setTimeout(() => {
            toast.remove();
        }, 3000);
    }

    clearToasts() {
        const toastContainer = document.getElementById('toastContainer');
        toastContainer.innerHTML = '';
    }

    clearForm() {
        const currentLanguage = this.baseLanguageSelect.value;

        // Clear source URL
        document.querySelector('input[placeholder="https://example.com"]').value = '';

        // Clear Google docs URL
        document.querySelector('input[placeholder="Google docs URL"]').value = '';

        // Clear alt titles
        const altTitles = document.getElementById('alt-titles');
        if (altTitles) {
            altTitles.innerHTML = '';
        }

        // Reset date picker
        const dateDisplay = document.getElementById('selectedDate');
        if (dateDisplay) {
            dateDisplay.textContent = 'No date selected';
        }

        // Reset era select to Standard
        const eraSelect = document.getElementById('eraSelect');
        if (eraSelect) {
            eraSelect.value = 'Standard';
        }

        // Clear historical year input
        const historicalYearInput = document.getElementById('historicalYearInput');
        if (historicalYearInput) {
            historicalYearInput.value = '';
        }

        // Uncheck all radio buttons
        this.typeRadios.forEach(radio => {
            radio.checked = false;
        });

        // Hide and reset pecha options
        this.pechaOptionsContainer.classList.remove('visible');
        this.pechaOptions.value = '';

        // Clear any error states
        this.clearErrors();

        // Reinitialize the form with the current language
        this.initializeFields(currentLanguage);
    }
}

// Initialize the form
new LocalizedForm();