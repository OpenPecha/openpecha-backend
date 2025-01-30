// Utility: returns whether all existing rows in a container are filled
function allRowsFilled(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return true; // If no container, trivially "filled"

    const rows = container.querySelectorAll(".localized-row");
    for (let row of rows) {
        const langInput = row.querySelector(".lang-input");
        const valInput = row.querySelector(".val-input");
        // language code or value might be empty => return false
        if (!langInput.value.trim() || !valInput.value.trim()) {
            return false;
        }
    }
    return true;
}

// Creates a single row {language, value}. If it's the first row, we make it "en" & disabled
function createLocalizedRow(isFirstRow = false) {
    const row = document.createElement("div");
    row.className = "localized-row";

    // Language code input
    const langInput = document.createElement("input");
    langInput.type = "text";
    langInput.className = "lang-input";
    if (isFirstRow) {
        langInput.value = "en";
        langInput.disabled = true;
    } else {
        langInput.placeholder = "Lang code (e.g., bo)";
    }

    // Value input
    const valInput = document.createElement("input");
    valInput.type = "text";
    valInput.placeholder = "Value";
    valInput.className = "val-input";

    row.appendChild(langInput);
    row.appendChild(valInput);

    return row;
}

// Adds a row to a localized container only if existing rows are filled
function attemptAddLocalizedRow(containerId) {
    // Check if all existing rows are filled
    if (!allRowsFilled(containerId)) {
        alert("Please fill out all existing language rows before adding a new one.");
        return;
    }
    // If filled, add a new row
    addLocalizedRow(containerId, false);
}

function addLocalizedRow(containerId, isFirstRow = false) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const row = createLocalizedRow(isFirstRow);
    container.appendChild(row);
}

// Adds an alt-title block (which is an array item of localized container)
function addAltTitlesBlock() {
    const altTitlesArray = document.getElementById("alt-titles-array");

    const block = document.createElement("div");
    block.className = "alt-title-block";

    // localized-container
    const container = document.createElement("div");
    container.className = "localized-container";
    container.id = `alt_title_${Date.now()}`;
    block.appendChild(container);

    // Add the + language button
    const addLangBtn = document.createElement("button");
    addLangBtn.type = "button";
    addLangBtn.textContent = "+ Language";
    addLangBtn.onclick = () => attemptAddLocalizedRow(container.id);

    block.appendChild(addLangBtn);
    altTitlesArray.appendChild(block);

    // Initially add one row (with "en")
    addLocalizedRow(container.id, true);
}

// Gathers data from a localized container -> { lang: value, ... }
function gatherLocalizedData(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return {};

    const rows = container.querySelectorAll(".localized-row");
    const data = {};

    rows.forEach((row) => {
        const langInput = row.querySelector(".lang-input");
        const valInput = row.querySelector(".val-input");
        if (langInput && valInput && langInput.value.trim()) {
            data[langInput.value.trim()] = valInput.value.trim();
        }
    });
    return data;
}

// Gathers data from an array of alt-title blocks
function gatherAltTitles() {
    const altTitlesArray = document.getElementById("alt-titles-array");
    const blocks = altTitlesArray.querySelectorAll(".alt-title-block");
    const result = [];

    blocks.forEach((block) => {
        const container = block.querySelector(".localized-container");
        if (container) {
            const obj = gatherLocalizedData(container.id);
            if (Object.keys(obj).length > 0) {
                result.push(obj);
            }
        }
    });
    return result;
}

// Handle Submit
async function handleSubmit() {
    // Build an object with all fields

    const author = gatherLocalizedData("author-container");
    const date = document.getElementById("date").value.trim();
    const source = document.getElementById("source").value.trim();
    const presentation = gatherLocalizedData("presentation-container");
    const usage_title = gatherLocalizedData("usage-container");
    const title = gatherLocalizedData("title-container");
    const long_title = gatherLocalizedData("longtitle-container");
    const alt_titles = gatherAltTitles();
    const version_of = document.getElementById("version_of").value;
    const translation_of = document.getElementById("translation_of").value;
    const commentary_of = document.getElementById("commentary_of").value
    const language = document.getElementById("language").value.trim();
    const docLink = document.getElementById("docLink").value.trim();

    const docId = extractDocIdFromLink(docLink);
    let blob;

    if (docId) {
        try {
            blob = await downloadDoc(docId);
        } catch (err) {
            alert("Download failed: " + err.message);
            return;
        }
    } else {
        alert("Invalid Google Docs link");
        return;
    }

    if (!author.en || !source || !title.en || !long_title.en || !language) {
        alert("Please fill in all required fields.");
        return;
    }

    const metadata = {
        author,
        date,
        source,
        presentation,
        document_id: docId,
        usage_title,
        title,
        long_title,
        alt_titles,
        language,
        translation_of,
        commentary_of,
        version_of
    };

    Object.keys(metadata).forEach((key) => {
        if (metadata[key] === null || metadata[key] === undefined || metadata[key].length === 0) {
            delete metadata[key];
        }
    });

    console.log(metadata)

    const formData = new FormData();
    formData.append("text", blob, `text_${docId}.docx`); // Binary file
    formData.append("metadata", JSON.stringify(metadata)); // JSON metadata

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

// INITIAL SETUP - Add default "en" row to each localized container
window.onload = () => {
    const containers = [
        "author-container",
        "presentation-container",
        "usage-container",
        "title-container",
        "longtitle-container"
    ];
    containers.forEach((cId) => addLocalizedRow(cId, true)); // create initial "en" row
};

function extractDocIdFromLink(docLink) {
    // Typical Docs link: https://docs.google.com/document/d/<DOC_ID>/edit
    // We want the <DOC_ID> after "/d/" and before "/edit"
    const regex = /\/d\/([^/]+)/;
    const match = docLink.match(regex);
    return match ? match[1] : null;
}