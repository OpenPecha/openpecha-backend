class UpdateMetadata {
    constructor() {
        this.isLoading = false;
        this.selectedPechaId = null;

        this.initialize();
    }

    async initialize() {
        try {
            this.API_ENDPOINT = await getApiEndpoint();
            this.setupElements();
            this.setupEventListeners();
            await this.fetchPechaOptions();
            this.initializeSearchUI();
            await this.fetchLanguages();

        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('Failed to initialize. Please refresh the page.', 'error');
        }
    }

    setupElements() {
        this.pechaSelectionContainer = document.getElementById("pechaSelectionContainer");
        this.pechaSelector = document.getElementById("pechaSelector");
        this.baseLanguageSelect = document.getElementById("baseLanguage");
        this.baseLanguageLoader = document.getElementById("baseLanguageLoader");
        this.formContent = document.getElementById("formContent");
        this.formGroups = document.querySelectorAll(".form-group");
        this.sourceUrl = document.getElementById("sourceUrl");
        this.source = document.getElementById("source");
        this.addAltTitleButton = document.getElementById("addAltTitle");
        this.searchContainers = document.querySelectorAll('.select-search-container');
        this.parentOptionsContainer = document.getElementById("parentOptionsContainer");
        this.parentSelect = document.getElementById("parent");
        this.parentLoadingSpinner = document.getElementById("parentLoadingSpinner");
        this.typeRadios = document.querySelectorAll(
            'input[name="type"]'
        );

        this.annotationOptionsContainer = document.getElementById("annotationAlignmentContainer");
        this.annotationLoadingSpinner = document.getElementById("annotationLoadingSpinner");
        this.updateButton = document.getElementById("updateButton")
        this.updateBtnText = document.querySelector(".update-button-text");
        this.updatingSpinner = updateButton.querySelector(".spinner")
        this.updating = false;
        this.languageOptions = [];

        // Initially hide parent selection
        this.parentOptionsContainer.style.display = "none";


    }

    setupEventListeners() {
        this.pechaSelector.addEventListener("change", async () => {
            const pechaId = this.pechaSelector.value;
            if (pechaId) {
                this.selectedPechaId = pechaId;
                const metadata = await this.fetchPechaMetadata(pechaId);
                this.populateMetadata(metadata)
            }
        });

        this.sourceUrl.addEventListener("input", this.handleSourceInput.bind(this));
        this.source.addEventListener("input", this.handleSourceInput.bind(this));
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
                const selectedType = radio.value;

                if (selectedType !== 'root') {
                    // Show parent selection for non-root types
                    this.fetchRelatedPechas(selectedType);
                } else {
                    // Hide and reset parent selection when root is selected
                    this.parentOptionsContainer.style.display = "none";
                    this.parentSelect.innerHTML = '<option value="">Select parent pecha</option>';
                }
            });
        });

        // Update Button
        this.updateButton.addEventListener("click", () => {
            this.handlePechaUpdate();
        });
    }

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
        searchTerm = searchTerm.trim().toLowerCase();
        resultsContainer.innerHTML = '';
        Array.from(select.options).forEach(option => {
            if ((searchTerm.trim() === '' || option.text.toLowerCase().includes(searchTerm))) {
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
        if (resultsContainer.innerHTML === '') {
            resultsContainer.innerHTML = '<div class="search-item" value="">No results found</div>';
        }
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

    createLocalizationInput(container, language = "", isFirst = false, isRequired = false, isTitle = false) {
        const inputContainer = document.createElement("div");
        inputContainer.className = "input-container";

        const inputGroup = document.createElement("div");
        inputGroup.className = "input-group";

        const inputWrapper = document.createElement("div");
        inputWrapper.className = "input-wrapper";

        const input = document.createElement("input");
        input.type = "text";
        input.placeholder = "Enter text";

        input.setAttribute("required", "");
        const langSelect = document.createElement("select");
        let temp = `<option value="">Language</option>`;
        this.languageOptions.forEach(lang => {
            temp += `<option value="${lang.code}">${lang.name}</option>`;
        });
        langSelect.innerHTML = temp;
        langSelect.setAttribute("required", "");
        langSelect.value = language;

        inputWrapper.appendChild(input);
        inputWrapper.appendChild(langSelect);
        inputGroup.appendChild(inputWrapper);

        if (!isFirst && (!isTitle || !isRequired)) {
            const removeButton = document.createElement("button");
            removeButton.type = "button";
            removeButton.className = "remove-btn";
            removeButton.title = "remove";
            removeButton.innerHTML = '<i class="fa fa-minus" aria-hidden="true"></i>';
            removeButton.addEventListener("click", () => {
                inputContainer.remove();
            });
            inputGroup.appendChild(removeButton);
        }

        inputContainer.appendChild(inputGroup);
        container.appendChild(inputContainer);
    }

    toggleUpdateButtonState(updating) {
        this.updating = updating;
        this.updateButton.disabled = updating;
        this.updateBtnText.textContent = updating ? 'Updating...' : 'Update';
        this.updatingSpinner.style.display = updating ? 'inline-block' : 'none';

        this.formGroups.forEach(group => {
            group.classList.toggle('disabled', updating);
        });
        this.baseLanguageSelect.disabled = updating;
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
        addLocalizationBtn.innerHTML = '<i class="fas fa-plus-circle"></i> Add Title';
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

    async fetchPechaMetadata(pecha_id) {
        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${pecha_id}`, {
                method: 'GET',
                headers: {
                    'accept': 'application/json'
                }
            });
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

            const metadata = await response.json();
            return metadata;
        } catch (error) {
            console.error('Error fetching pecha metadata:', error);
            this.showToast('Unable to fetch pecha metadata. Please try again later.', 'error');
        }
    }

    populateMetadata(metadata) {
        // Clear form first
        this.clearForm();

        // Set base language if available
        if (metadata.language) {
            this.baseLanguageSelect.value = this.languageOptions.find(lang => lang.code === metadata.language).code;
        }

        // Set type and parent if available
        if (metadata.type) {
            const typeRadio = document.querySelector(`input[name="type"][value="${metadata.type}"]`);
            if (typeRadio) {
                typeRadio.checked = true;

                // If type is not root, fetch and set parent
                if (metadata.type !== 'root' && metadata.parent) {
                    this.fetchRelatedPechas(metadata.type).then(() => {
                        this.parentSelect.value = metadata.parent;
                    });
                }
            }
        }

        // Populate titles
        if (metadata.title) {
            const titleContainer = document.querySelector('.form-group[data-field="title"] .localizations');
            if (titleContainer) {
                titleContainer.innerHTML = ''; // Clear existing
                Object.entries(metadata.title).forEach(([lang, value], index) => {
                    this.createLocalizationInput(titleContainer, lang, index === 0, lang === 'bo' || lang === 'en' || lang === 'lzh', true);
                    const input = titleContainer.querySelector('.input-container:last-child input[type="text"]');
                    if (input) input.value = value;
                });
            }
        }

        // Populate long titles
        if (metadata.long_title) {
            const longTitleContainer = document.querySelector('.form-group[data-field="long_title"] .localizations');
            if (longTitleContainer) {
                longTitleContainer.innerHTML = ''; // Clear existing
                Object.entries(metadata.long_title).forEach(([lang, value], index) => {
                    this.createLocalizationInput(longTitleContainer, lang, true, index === 0);
                    const input = longTitleContainer.querySelector('.input-container:last-child input[type="text"]');
                    if (input) input.value = value;
                });
            }
        }

        // Populate authors
        if (metadata.author) {
            const authorContainer = document.querySelector('.form-group[data-field="author"] .localizations');
            if (authorContainer) {
                authorContainer.innerHTML = ''; // Clear existing
                Object.entries(metadata.author).forEach(([lang, value], index) => {
                    this.createLocalizationInput(authorContainer, lang, true, index === 0);
                    const input = authorContainer.querySelector('.input-container:last-child input[type="text"]');
                    if (input) input.value = value;
                });
            }
        }

        // Set source if available
        if (metadata.source) {
            const sourceInput = document.getElementById('source');
            if (sourceInput) sourceInput.value = metadata.source;
        }

        // Set source URL if available
        if (metadata.source_url) {
            const sourceUrlInput = document.getElementById('sourceUrl');
            if (sourceUrlInput) sourceUrlInput.value = metadata.source_url;
        }

        // Set document ID if available
        if (metadata.document_id) {
            const docUrlInput = document.querySelector('input[placeholder="Google docs URL"]');
            if (docUrlInput) docUrlInput.value = `https://docs.google.com/document/d/${metadata.document_id}/edit`;
        }

        // Populate alternate titles if any
        if (metadata.alt_titles && metadata.alt_titles.length > 0) {
            const altTitlesContainer = document.getElementById('alt-titles');
            if (altTitlesContainer) {
                altTitlesContainer.innerHTML = ''; // Clear existing

                metadata.alt_titles.forEach(altTitle => {
                    // Add a new alt title group
                    this.addAltTitles(metadata.language || 'bo');

                    // Get the last added group
                    const lastGroup = altTitlesContainer.querySelector('.alt-title-group:last-child');
                    if (lastGroup) {
                        const localizationsDiv = lastGroup.querySelector('.localizations');
                        if (localizationsDiv) {
                            localizationsDiv.innerHTML = ''; // Clear default inputs

                            // Add each language version
                            Object.entries(altTitle).forEach(([lang, value], index) => {
                                this.createLocalizationInput(localizationsDiv, lang, true, index === 0);
                                const input = localizationsDiv.querySelector('.input-container:last-child input[type="text"]');
                                if (input) input.value = value;
                            });
                        }
                    }
                });
            }
        }

        // Set composition date if exists
        if (metadata.composition_date) {
            const dateDisplay = document.getElementById('selectedDate');
            if (dateDisplay) {
                // Handle different date formats
                if (typeof metadata.composition_date === 'string') {
                    // Simple string date
                    dateDisplay.textContent = metadata.composition_date;
                } else if (metadata.composition_date.standard) {
                    // Standard date format
                    dateDisplay.textContent = metadata.composition_date.standard;

                    // Set era to Standard
                    const eraSelect = document.getElementById('eraSelect');
                    if (eraSelect) eraSelect.value = 'Standard';
                } else if (metadata.composition_date.year_start) {
                    // Historical date format
                    let dateText = metadata.composition_date.year_start.toString();
                    if (metadata.composition_date.year_end && metadata.composition_date.year_end !== metadata.composition_date.year_start) {
                        dateText += ` to ${metadata.composition_date.year_end}`;
                    }
                    dateDisplay.textContent = dateText;

                    // Set era (BCE/CE)
                    const eraSelect = document.getElementById('eraSelect');
                    if (eraSelect) {
                        eraSelect.value = metadata.composition_date.era || 'CE';
                    }

                    // Show historical picker
                    const standardPicker = document.getElementById('standardPicker');
                    const historicalPicker = document.getElementById('historicalPicker');
                    if (standardPicker && historicalPicker) {
                        standardPicker.classList.add('hidden');
                        historicalPicker.classList.remove('hidden');

                        // Set year values
                        const yearStartInput = document.getElementById('historicalYearStart');
                        const yearEndInput = document.getElementById('historicalYearEnd');
                        if (yearStartInput) yearStartInput.value = metadata.composition_date.year_start;
                        if (yearEndInput) yearEndInput.value = metadata.composition_date.year_end || metadata.composition_date.year_start;
                    }
                }
            }
        }


        this.enableForm();
        this.showToast('Metadata loaded successfully', 'info');
    }

    clearTitleFields() {
        // Clear all title input fields
        const titleGroup = document.querySelector('.form-group[data-field="title"]');
        if (titleGroup) {
            const inputs = titleGroup.querySelectorAll('input[type="text"]');
            inputs.forEach(input => {
                input.value = '';
                input.removeAttribute('readonly');
                input.placeholder = 'Enter text';
            });
        }
    }

    async fetchPechaOptions() {
        let body = { filter: {} };
        try {
            this.disableForm();
            this.pechaSelectionContainer.style.display = "none";
            this.toggleLoadingSpinner(true, this.pechaSelectionContainer, this.pechaSelectionContainer.parentElement.querySelector('.loading-spinner'));

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
            this.toggleLoadingSpinner(false, this.pechaSelectionContainer, this.pechaSelectionContainer.parentElement.querySelector(".loading-spinner"));
            this.pechaSelectionContainer.style.display = "block";
            allPechas.forEach(pecha => {
                const title = pecha.title[pecha.language] ?? pecha.title.bo;
                const option = new Option(`${pecha.id} - ${title}`, pecha.id);
                this.pechaSelectionContainer.querySelector('select').add(option.cloneNode(true));
            });
        } catch (error) {
            this.toggleLoadingSpinner(false, this.pechaSelectionContainer, this.pechaSelectionContainer.parentElement.querySelector(".loading-spinner"));
            console.error("Error loading pecha options:", error);
            this.showToast("Unable to load pecha options. Please try again later.", "error");
        } finally {
            this.toggleLoadingSpinner(false, this.pechaSelectionContainer, this.pechaSelectionContainer.parentElement.querySelector(".loading-spinner"));
        }
    }

    async fetchRelatedPechas(type) {
        const filters = {
            commentary: { "field": "type", "operator": "==", "value": "root" },
            version: { "field": "type", "operator": "==", "value": "root" },
            translation: { "field": "type", "operator": "==", "value": "root" }
        };
        const body = { filter: filters[type] || {} };

        try {
            this.parentOptionsContainer.style.display = "none";
            this.toggleLoadingSpinner(true, this.parentOptionsContainer, this.parentLoadingSpinner);

            let allPechas = [];
            let currentPage = 1;
            let hasMorePages = true;
            const limit = 100;

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

            this.toggleLoadingSpinner(false, this.parentOptionsContainer, this.parentLoadingSpinner);
            this.parentOptionsContainer.style.display = "block";
            this.populatePechaDropdown(allPechas);
        } catch (error) {
            this.toggleLoadingSpinner(false, this.parentOptionsContainer, this.parentLoadingSpinner);
            console.error("Error loading pecha options:", error);
            this.showToast("Unable to load pecha options. Please try again later.", "error");
        }
    }

    populatePechaDropdown(pechas) {
        while (this.parentSelect.options.length > 1) {
            this.parentSelect.remove(1);
        }
        pechas.forEach(pecha => {
            const title = pecha.title[pecha.language] ?? pecha.title.bo;
            const option = new Option(`${pecha.id} - ${title}`, pecha.id);
            this.parentSelect.add(option.cloneNode(true));
        });
    }

    async fetchLanguages() {
        try {
            this.toggleLoadingSpinner(true, this.baseLanguageSelect, this.baseLanguageLoader);
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
            let temp = `<option value="">Language</option>`;
            this.languageOptions.forEach(lang => {
                temp += `<option value="${lang.code}">${lang.name}</option>`;
            });
            this.baseLanguageSelect.innerHTML = temp;

            return data;
        } catch (error) {
            console.error('Error fetching languages:', error);
            this.showToast('Failed to load languages. Please refresh the page.', 'error');
        } finally {
            this.toggleLoadingSpinner(false, this.baseLanguageSelect, this.baseLanguageLoader);
        }
    }

    collectFormData() {
        const formData = {
            metadata: {
                language: this.baseLanguageSelect.value,
                type: document.querySelector('input[name="type"]:checked').value
            }
        };

        // Add parent if type is not root
        if (formData.metadata.type !== 'root') {
            const parentId = this.parentSelect.value;
            if (parentId) {
                formData.metadata.parent = parentId;
            }
        }

        // Collect localized fields
        document.querySelectorAll(".form-group[data-field]").forEach(group => {
            const fieldName = group.dataset.field;
            const localizations = {};

            group.querySelectorAll(".input-container").forEach(container => {
                const input = container.querySelector("input, textarea");
                const select = container.querySelector("select");

                if (input && input.value && select && select.value) {
                    const lang = select.value;
                    localizations[lang] = input.value;
                }
            });

            if (Object.keys(localizations).length > 0) {
                formData.metadata[fieldName] = localizations;
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
            formData.metadata.alt_titles = alt_titles;
        // Collect non-localized fields
        const selectedDate = document.getElementById("selectedDate").innerHTML;
        if (selectedDate && selectedDate !== "No date selected") {
            formData.metadata.date = selectedDate;
        }
        formData.metadata.source_url = this.sourceUrl.value || null;
        formData.metadata.source = this.source.value || null;

        // Collect Google Docs id 
        formData.metadata.document_id = this.extractDocIdFromLink(document.querySelector('input[placeholder="Google docs URL"]').value);

        return formData
    }

    validateRequiredFields(formData) {
        const metadata = formData.metadata;
        function isValidURL(url) {
            try {
                new URL(url);
                return true;
            } catch (error) {
                return false;
            }
        }

        // Validate type
        if (!metadata.type) {
            const typeGroup = document.querySelector('.form-group:has(input[name="type"])');
            if (typeGroup) {
                typeGroup.classList.add('error');
            }
            this.showToast("Type is required", "error");
            return false;
        } else {
            const typeGroup = document.querySelector('.form-group:has(input[name="type"])');
            if (typeGroup) {
                typeGroup.classList.remove('error');
            }
        }

        // Validate parent for non-root types
        if (metadata.type !== 'root' && !metadata.parent) {
            this.parentSelect.classList.add('error');
            this.parentOptionsContainer.classList.add('error');
            this.showToast("Parent pecha is required for non-root types", "error");
            return false;
        } else {
            this.parentSelect.classList.remove('error');
            this.parentOptionsContainer.classList.remove('error');
        }

        // Check author in English
        if (!metadata.author) {
            this.highlightError('author');
            this.showToast("Author is required", "error");
            return false;
        }

        // Check source URL and source
        if (!metadata.source_url && !metadata.source) {
            this.sourceUrl.closest('.input-wrapper').classList.add('error');
            this.source.closest('.input-wrapper').classList.add('error');
            this.showToast("Either Source URL or Source is required", "error");
            return false;
        } else {
            // Validate Source URL if provided
            if (metadata.source_url && !isValidURL(metadata.source_url)) {
                this.sourceUrl.closest('.input-wrapper').classList.add('error');
                this.showToast("Invalid URL format", "error");
                return false;
            } else {
                this.sourceUrl.closest('.input-wrapper').classList.remove('error');
            }

            // Validate Source if provided
            if (metadata.source && typeof metadata.source !== "string") {
                this.source.closest('.input-wrapper').classList.add('error');
                this.showToast("Source must be a text string", "error");
                return false;
            } else {
                this.source.closest('.input-wrapper').classList.remove('error');
            }
        }

        // Check title has all required languages
        const baseLanguage = this.baseLanguageSelect.value;
        if (!metadata.title) {
            this.highlightError('title', 0);
            this.showToast("Title is required", "error");
            return false;
        }

        // Get all required languages for title
        const requiredLangs = new Set([baseLanguage, "bo", "en"]);
        const missingLangs = [];

        // Check each required language
        for (const lang of requiredLangs) {
            if (!metadata.title[lang]) {
                missingLangs.push(lang);
            }
        }

        if (missingLangs.length > 0) {
            // Map language codes to their positions in the form
            const langPositions = {};
            const containers = document.querySelectorAll('.form-group[data-field="title"] .input-container');
            containers.forEach((container, index) => {
                const select = container.querySelector('select');
                if (select) {
                    langPositions[select.value] = index;
                }
            });

            // Highlight the first missing language field
            const firstMissing = missingLangs[0];
            const index = langPositions[firstMissing] || 0;
            this.highlightError('title', index);

            // Show descriptive error message
            const langNames = {
                'bo': 'Tibetan',
                'en': 'English',
                [baseLanguage]: `base language (${baseLanguage})`
            };
            const missingNames = missingLangs.map(lang => langNames[lang] || lang);
            this.showToast(`Title in ${missingNames.join(', ')} is required`, "error");
            return false;
        }
        // Check long title in English
        if (!metadata.long_title) {
            this.highlightError('long_title');
            this.showToast("Long title is required", "error");
            return false;
        }

        // Check base language
        if (!metadata.language) {
            this.baseLanguageSelect.classList.add('error');
            this.showToast("Base language is required", "error");
            return false;
        }

        if (!metadata.document_id) {
            document.querySelector('input[placeholder="Google docs URL"]').closest('.input-wrapper').classList.add('error');
            this.showToast("Enter valid Google docs URL", "error");
            return false;
        }

        return true;
    }

    highlightError(fieldName, fieldIndex = 0) {
        const formGroup = document.querySelector(`.form-group[data-field="${fieldName}"]`);
        if (formGroup) {
            const containers = formGroup.querySelectorAll('.input-container');
            if (containers[fieldIndex]) {
                containers[fieldIndex].querySelector('.input-wrapper').classList.add('error');
            }
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
        // Typical Docs link: https://docs.google.com/document/d/<DOC_ID>/edit
        // We want the <DOC_ID> after "/d/" and before "/edit"
        const regex = /\/d\/([^/]+)/;
        const match = docLink.match(regex);
        return match ? match[1] : null;
    }

    async handlePechaUpdate() {
        this.clearErrors();
        const pechaData = this.collectFormData();
        console.log("data", pechaData);
        if (!this.validateRequiredFields(pechaData))
            return;
        this.toggleUpdateButtonState(true);

        try {
            const response = await fetch(`${this.API_ENDPOINT}/metadata/${this.selectedPechaId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ metadata: pechaData.metadata })
            });
            if (!response.ok) {
                const json = await response.text();
                console.log("json:::", json)
                const text = JSON.parse(json);
                throw new Error(`${text.error}`);
            }

            const jsonResponse = await response.json();
            this.showToast(`${jsonResponse?.message}`, "success");
            // this.clearForm();

        } catch (error) {
            console.error('Error in handlePechaUpdate:', error);
            this.showToast(error, "error");
        } finally {
            this.toggleUpdateButtonState(false);
        }
    }

    async submitFormData(formData) {
        return fetch(`${this.API_ENDPOINT}/pecha`, {
            method: "POST",
            body: formData
        });
    }

    toggleLoadingSpinner(isLoading, loadingContainer, loader) {
        if (isLoading) {
            loader.classList.add('active');
            loadingContainer.classList.add('loading');
        } else {
            loader.classList.remove('active');
            loadingContainer.classList.remove('loading');
        }
    }

    handleSourceInput(event) {
        if (event.target.value) {
            if (event.target === this.sourceUrl) {
                this.source.value = "";
            } else {
                this.sourceUrl.value = "";
            }
        }
    }

    disableForm() {
        this.formContent.classList.add('disabled');
    }

    enableForm() {
        this.formContent.classList.remove('disabled');
    }

    showToast(message, type) {
        const toastContainer = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `${this.getToastIcon(type)} ${message}`;
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

        // Reset type to root
        const rootRadio = document.querySelector('input[name="type"][value="root"]');
        if (rootRadio) {
            rootRadio.checked = true;
        }

        // Hide and reset parent selection
        this.parentOptionsContainer.style.display = "none";
        this.parentSelect.innerHTML = '<option value="">Select parent pecha</option>';

        // Clear any error states
        this.clearErrors();
    }
}

// Initialize the form
document.addEventListener('DOMContentLoaded', () => new UpdateMetadata());
