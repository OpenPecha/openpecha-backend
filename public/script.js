class LocalizedForm {
    constructor() {
        this.baseLanguageSelect = document.getElementById("baseLanguage");
        this.formContent = document.getElementById("formContent");
        this.pechaOptionsContainer = document.getElementById(
            "pechaOptionsContainer"
        );
        this.pechaOptions = document.getElementById("pechaOptions");
        this.typeRadios = document.querySelectorAll(
            'input[name="documentType"]'
        );
        this.publishButton = document.getElementById("publishButton")
        this.setupEventListeners();
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
        });

        this.publishButton.addEventListener("click", () => {
            this.handlePublish();
        });
        // Add Localization Buttons
        document.querySelectorAll(".add-localization").forEach((button) => {
            button.addEventListener("click", (e) => {
                const formGroup = e.target.closest(".form-group");
                this.addLocalization(formGroup);
            });
        });

        // Type Radio Selection
        this.typeRadios.forEach((radio) => {
            radio.addEventListener("change", () => {
                if (radio.checked) {
                    this.pechaOptionsContainer.classList.add("visible");
                    this.fetchPechaOptions();
                }
            });
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

    createLocalizationInput(container, language, isFirst = false) {
        const inputContainer = document.createElement("div");
        inputContainer.className = "input-container";

        const inputGroup = document.createElement("div");
        inputGroup.className = "input-group";

        const inputWrapper = document.createElement("div");
        inputWrapper.className = "input-wrapper";

        const isTextarea =
            container.closest(".form-group").dataset.field === "presentation";
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
        langSelect.innerHTML = `
    <option value="">Language</option>
    <option value="en">English</option>
    <option value="fr">French</option>
    <option value="bo">Tibetan</option>
  `;
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
            alert(
                "Please fill in all existing fields before adding a new localization"
            );
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

    async fetchPechaOptions() {
        try {
            const response = await fetch(
                "https://api-aq25662yyq-uc.a.run.app/pecha/"
            );
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

        // Collect non-localized fields
        metadata.data = document.querySelector('input[type="date"]').value;
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
            errors.push('longTitle');
            this.highlightError('longTitle');
        }

        // Check base language
        if (!metadata.language) {
            errors.push('language');
            this.baseLanguageSelect.classList.add('error');
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

    async handlePublish() {
        // Clear any existing error states
        this.clearErrors();

        // Collect form data
        const metadata = this.collectFormData();

        // Validate required fields
        if (!this.validateRequiredFields(metadata)) {
            alert("Please fill in all required fields.");
            return;
        }

        console.log("Form Data:", metadata);
        let blob;
        if (metadata.document_id) {
            try {
                blob = await downloadDoc(metadata.document_id);
            } catch (err) {
                alert("Download failed: " + err.message);
                return;
            }
        } else {
            alert("Invalid Google Docs link");
            return;
        }
        // If validation passes, log the data
        console.log("Form Data:", JSON.stringify(metadata, null, 2));

        const formData = new FormData();
        formData.append("text", blob, `text_${metadata.document_id}.docx`); // Binary file
        formData.append("metadata", JSON.stringify(metadata)); // JSON metadata
        console.log("form data ::::",formData)
        try {
            const response = await fetch("https://api-aq25662yyq-uc.a.run.app/publish/", {
                // const response = await fetch("http://127.0.0.1:5001/pecha-backend/us-central1/api/publish/", {
                method: "POST",
                body: formData,
            });

            if (response.ok) {
                const jsonResponse = await response.json();
                const pechaId = jsonResponse.pecha_id;
                const serializedData = jsonResponse.data;

                const blob = new Blob([JSON.stringify(serializedData, null)], {
                    type: "application/json",
                });

                const downloadUrl = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = downloadUrl;
                a.download = `${pechaId}.json`;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(downloadUrl);

                alert("File and metadata successfully submitted!");
            } else {
                const error = await response.text();
                alert(`Failed to submit: ${error}`);
            }
        } catch (err) {
            console.error("Error submitting form:", err);
            alert(`Error: ${err.message}`);
        }
    }
}

// Initialize the form
new LocalizedForm();