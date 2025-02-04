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
function createLocalizedRow(isFirstRow = false, placeholder) {
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
  valInput.placeholder = "Localized Value";
  valInput.className = "val-input";

  row.appendChild(langInput);
  row.appendChild(valInput);

  // Remove button (except for the first "en" row)
  if (!isFirstRow) {
    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.textContent = "❌";
    removeButton.className = "remove-lang";
    removeButton.onclick = () => row.remove(); // Remove row on click
    row.appendChild(removeButton);
  }

  return row;
}

// Adds a row to a localized container only if existing rows are filled
function attemptAddLocalizedRow(containerId) {
  // Check if all existing rows are filled
  if (!allRowsFilled(containerId)) {
    alert(
      "Please fill out all existing language rows before adding a new one."
    );
    return;
  }
  // If filled, add a new row
  addLocalizedRow(containerId, false);
  // attachSaveListeners();
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
  addLangBtn.textContent = "➕";
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

// INITIAL SETUP - Add default "en" row to each localized container
window.onload = () => {
  const containers = [
    "author-container",
    "presentation-container",
    "usage-container",
    "title-container",
    "longtitle-container",
  ];
  containers.forEach((cId) => addLocalizedRow(cId, true)); // create initial "en" row
};

// Ensure only one of version_of, translation_of, or commentary_of is selected
function enforceMutualExclusivity(selectedId) {
  const fields = ["version_of", "translation_of", "commentary_of"];
  fields.forEach((id) => {
    if (id !== selectedId) {
      document.getElementById(id).value = "";
    }
  });
}

async function fetchPechaOptions() {
  try {
    const response = await fetch("https://api-aq25662yyq-uc.a.run.app/pecha/");
    if (!response.ok) {
      throw new Error(`Failed to fetch data: ${response.statusText}`);
    }
    const pechas = await response.json();
    const isUpdatePage = window.location.pathname.includes("update.html");
    const dropdowns = isUpdatePage
      ? ["published_text"]
      : ["commentary_of", "version_of", "translation_of"];
    dropdowns.forEach((id) => {
      const select = document.getElementById(id);
      select.innerHTML = `<option value=""></option>`;

      pechas.forEach((pecha) => {
        const option = document.createElement("option");
        option.value = pecha.id;
        option.textContent = `${pecha.title} (${pecha.id})`;
        select.appendChild(option);
      });
    });

    console.log("Dropdowns populated successfully.");
  } catch (error) {
    console.error("Error loading pecha options:", error);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  fetchPechaOptions();
  if (!window.location.pathname.includes("update.html")) {
    ["version_of", "translation_of", "commentary_of"].forEach((id) => {
      const element = document.getElementById(id);
      if (element) {
        element.addEventListener("change", function () {
          enforceMutualExclusivity(id);
        });
      }
    });
  }
});
