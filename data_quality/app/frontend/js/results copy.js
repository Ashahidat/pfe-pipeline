const API_URL = "http://localhost:8000";

async function fetchResults() {
    const statusDiv = document.getElementById("status");
    const resultsSection = document.getElementById("resultsSection");

    try {
        const dag_run_id = localStorage.getItem("dag_run_id");
        if (!dag_run_id) {
            alert("Pas de DAG en cours");
            return;
        }

        statusDiv.innerText = "DAG en cours d'exécution...";

        let state = null;
        let attempts = 0;
        const maxAttempts = 60;

        while (state !== "success" && state !== "failed" && attempts < maxAttempts) {
            try {
                const res = await fetch(`${API_URL}/dag-status?dag_run_id=${encodeURIComponent(dag_run_id)}`);
                if (!res.ok) {
                    throw new Error(`HTTP error! status: ${res.status}`);
                }
                const data = await res.json();
                state = data?.state || null;
                console.log("État actuel du DAG:", state);

                if (state === "running" || !state) {
                    statusDiv.innerText = `DAG en cours d'exécution... (tentative ${attempts + 1}/${maxAttempts})`;
                    await new Promise(r => setTimeout(r, 5000));
                    attempts++;
                } else {
                    break;
                }
            } catch (error) {
                console.error("Erreur lors de la vérification du statut:", error);
                statusDiv.innerText = "Erreur de connexion au serveur";
                await new Promise(r => setTimeout(r, 5000));
                attempts++;
            }
        }

        console.log("État final:", state, "Tentatives:", attempts);

        if (state === "success") {
            statusDiv.innerText = "DAG terminé avec succès, récupération des résultats...";
            
            try {
                const res2 = await fetch(`${API_URL}/results`);
                if (!res2.ok) {
                    throw new Error(`HTTP error! status: ${res2.status}`);
                }
                const results = await res2.json();
                console.log("Résultats reçus:", results);
                
                if (!results || results.length === 0) {
                    statusDiv.innerText = "Aucun résultat disponible !";
                    return;
                }
                
                renderResultsJSON(results);
                statusDiv.style.display = "none";
                
            } catch (error) {
                console.error("Erreur lors de la récupération des résultats:", error);
                statusDiv.innerText = "Erreur lors de la récupération des résultats";
            }
            
        } else if (state === "failed") {
            statusDiv.innerText = "Le DAG a échoué !";
            resultsSection.style.display = "none";
            addBackButton(); // Ajouter le bouton même en cas d'échec
        } else {
            statusDiv.innerText = "Impossible de récupérer l'état du DAG dans le temps imparti.";
            resultsSection.style.display = "none";
            addBackButton(); // Ajouter le bouton même en cas de timeout
        }

    } catch (err) {
        console.error("Erreur fetchResults :", err);
        statusDiv.innerText = "Erreur lors de la récupération des résultats";
        resultsSection.style.display = "none";
        addBackButton(); // Ajouter le bouton même en cas d'erreur
    }
}

function addBackButton() {
    // Vérifier si le bouton existe déjà pour éviter les doublons
    if (document.getElementById('backButton')) {
        return;
    }
    
    const container = document.querySelector('.container');
    const backButton = document.createElement('button');
    backButton.id = 'backButton';
    backButton.className = 'back-button';
    backButton.textContent = '← Retour à l\'upload de fichier';
    backButton.onclick = function() {
        window.location.href = 'index.html'; // Remplacez par le nom de votre page d'upload
    };
    
    container.appendChild(backButton);
}

function renderResultsJSON(data) {
    const resultsSection = document.getElementById("resultsSection");
    resultsSection.innerHTML = "";
    
    console.log("Données à afficher:", data);

    function createCard(item) {
        const card = document.createElement("div");
        card.className = "result-card";

        const header = document.createElement("div");
        header.className = "header";

        const title = document.createElement("div");
        title.className = "card-title";
        title.innerText = item["alerte"] || item["type de test"] || "Test de validation";

        const status = document.createElement("div");
        status.className = "status " +
            (item.statut === "réussi" ? "status-success" :
             item.statut === "échoué" ? "status-failed" : "status-running");
        status.innerText = item.statut || "N/A";

        header.appendChild(title);
        header.appendChild(status);
        card.appendChild(header);

        // Affichage des informations principales
        const infoDiv = document.createElement("div");
        infoDiv.className = "card-info";
        
        const keysToSkip = ["exemples", "type de test", "alerte", "statut"];
        Object.entries(item).forEach(([key, val]) => {
            if (!keysToSkip.includes(key) && val !== undefined && val !== null) {
                const infoRow = document.createElement("div");
                infoRow.className = "info-row";
                
                const keySpan = document.createElement("span");
                keySpan.className = "info-key";
                keySpan.textContent = key + ":";
                
                const valueSpan = document.createElement("span");
                valueSpan.className = "info-value";
                valueSpan.textContent = typeof val === "object" ? JSON.stringify(val) : val.toString();
                
                infoRow.appendChild(keySpan);
                infoRow.appendChild(valueSpan);
                infoDiv.appendChild(infoRow);
            }
        });
        
        card.appendChild(infoDiv);

        // Affichage des exemples de manière structurée
        if (item.exemples && item.exemples.length > 0) {
            const examplesDiv = document.createElement("div");
            examplesDiv.className = "examples-section";
            
            const examplesTitle = document.createElement("h4");
            examplesTitle.textContent = "Exemples:";
            examplesDiv.appendChild(examplesTitle);
            
            item.exemples.forEach(example => {
                const exampleDiv = document.createElement("div");
                exampleDiv.className = "example-item";
                
                if (typeof example === "object" && example !== null) {
                    // Pour les objets (comme les doublons de ligne complète)
                    Object.entries(example).forEach(([exKey, exVal]) => {
                        const exRow = document.createElement("div");
                        exRow.className = "example-row";
                        
                        const exKeySpan = document.createElement("span");
                        exKeySpan.className = "example-key";
                        exKeySpan.textContent = exKey + ":";
                        
                        const exValueSpan = document.createElement("span");
                        exValueSpan.className = "example-value";
                        exValueSpan.textContent = exVal !== null && exVal !== undefined ? exVal.toString() : "N/A";
                        
                        exRow.appendChild(exKeySpan);
                        exRow.appendChild(exValueSpan);
                        exampleDiv.appendChild(exRow);
                    });
                } else {
                    // Pour les valeurs simples (emails, téléphones, etc.)
                    const simpleExample = document.createElement("div");
                    simpleExample.className = "simple-example";
                    simpleExample.textContent = example !== null && example !== undefined ? example.toString() : "N/A";
                    exampleDiv.appendChild(simpleExample);
                }
                
                examplesDiv.appendChild(exampleDiv);
            });
            
            card.appendChild(examplesDiv);
        }

        resultsSection.appendChild(card);
    }

    // Traitement des données selon leur format
    if (Array.isArray(data)) {
        console.log("Traitement d'un tableau de résultats:", data);
        data.forEach(item => createCard(item));
    } else if (typeof data === 'object' && data !== null) {
        console.log("Traitement d'un objet de résultats:", data);
        
        // Traitement des doublons
        if (data.duplicates && Array.isArray(data.duplicates)) {
            data.duplicates.forEach(item => createCard(item));
        }
        
        // Traitement des regex
        if (data.regex) {
            if (data.regex.regex && Array.isArray(data.regex.regex)) {
                data.regex.regex.forEach(item => createCard(item));
            } else if (Array.isArray(data.regex)) {
                data.regex.forEach(item => createCard(item));
            }
        }
        
        // Traitement d'autres propriétés potentielles
        Object.entries(data).forEach(([key, value]) => {
            if (key !== 'duplicates' && key !== 'regex' && Array.isArray(value)) {
                value.forEach(item => createCard(item));
            }
        });
    }
    
    // Si aucune carte n'a été créée, afficher un message
    if (resultsSection.children.length === 0) {
        resultsSection.innerHTML = '<div class="no-results">Aucun résultat à afficher</div>';
        console.log("Structure des données reçues:", JSON.stringify(data, null, 2));
    }
    
    // Ajouter le bouton de retour après l'affichage des résultats
    addBackButton();
}

// Lancer la récupération automatiquement
document.addEventListener('DOMContentLoaded', function() {
    console.log("Démarrage de la récupération des résultats...");
    fetchResults();
});