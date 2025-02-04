function getFormData() {
  return {
    author: gatherLocalizedData("author-container"),
    date: document.getElementById("date").value.trim(),
    source: document.getElementById("source").value.trim(),
    presentation: gatherLocalizedData("presentation-container"),
    usage_title: gatherLocalizedData("usage-container"),
    title: gatherLocalizedData("title-container"),
    long_title: gatherLocalizedData("longtitle-container"),
    alt_titles: gatherAltTitles(),
    version_of: document.getElementById("version_of").value,
    translation_of: document.getElementById("translation_of").value,
    commentary_of: document.getElementById("commentary_of").value,
    language: document.getElementById("language").value.trim(),
    document_id:
      extractDocIdFromLink(document.getElementById("docLink").value.trim()) ||
      "",
  };
}

async function handleSubmit() {
  metadata = getFormData();

  if (
    !metadata.author.en ||
    !metadata.source ||
    !metadata.title.en ||
    !metadata.long_title.en ||
    !metadata.language
  ) {
    alert("Please fill in all required fields.");
    return;
  }

  Object.keys(metadata).forEach((key) => {
    if (
      metadata[key] === null ||
      metadata[key] === undefined ||
      metadata[key].length === 0
    ) {
      delete metadata[key];
    }
  });

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

  console.log(metadata);

  const formData = new FormData();
  formData.append("text", blob, `text_${metadata.document_id}.docx`); // Binary file
  formData.append("metadata", JSON.stringify(metadata)); // JSON metadata

  try {
    const response = await fetch(
      "https://api-aq25662yyq-uc.a.run.app/publish/",
      {
        // const response = await fetch("http://127.0.0.1:5001/pecha-backend/us-central1/api/publish/", {
        method: "POST",
        body: formData,
      }
    );

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

async function handleUpdate() {
  const publishedTextSelect = document.getElementById("published_text");
  const udocLinkInput = document.getElementById("udocLink");

  if (!publishedTextSelect || !udocLinkInput) {
    throw new Error("Required form elements not found");
  }

  const publishTextId = publishedTextSelect.value.trim();
  const googleDocLink = udocLinkInput.value.trim();
  const docId = extractDocIdFromLink(googleDocLink);

  if (!publishTextId || !googleDocLink) {
    alert("Please fill in both fields before submitting.");
    return;
  }

  if (!docId) {
    alert("Invalid Google Docs link");
    return;
  }

  try {
    const blob = await downloadDoc(docId);
    if (!blob) {
      throw new Error("Failed to download document");
    }

    const formData = new FormData();
    formData.append("text", blob, `text_${docId}.docx`);
    formData.append("id", publishTextId);

    const response = await fetch(
      "https://api-aq25662yyq-uc.a.run.app/update-text/",
      {
        method: "POST",
        body: formData,
      }
    );

    if (response.ok) {
      alert("Document updated successfully!");
    } else {
      const errorText = await response.text();
      throw new Error(`Update failed: ${errorText}`);
    }
  } catch (error) {
    console.error("Error during update:", error);
    alert(`Error: ${error.message}`);
  }
}

function extractDocIdFromLink(docLink) {
  // Typical Docs link: https://docs.google.com/document/d/<DOC_ID>/edit
  // We want the <DOC_ID> after "/d/" and before "/edit"
  const regex = /\/d\/([^/]+)/;
  const match = docLink.match(regex);
  return match ? match[1] : null;
}
