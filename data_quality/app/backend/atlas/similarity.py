# similarity.py - Version debug ultra-simplifiée
import hashlib

def hash_partial(df, n=1000):
    """Version ultra simple du hash"""
    try:
        print("🎯 DEBUG HASH - Calcul en cours...")
        
        # 1. Juste les noms de colonnes triés
        columns_str = str(sorted(df.columns))
        simple_hash = hashlib.sha256(columns_str.encode()).hexdigest()[:16]
        
        print(f"   Colonnes: {df.columns}")
        print(f"   Hash (colonnes seulement): {simple_hash}")
        
        return simple_hash
        
    except Exception as e:
        print(f"❌ Erreur hash_partial: {e}")
        return "error_hash"

def hash_similarity(h1, h2):
    """Version ultra simple de similarité"""
    print(f"🎯 DEBUG SIMILARITY - Comparaison:")
    print(f"   Hash1: {h1}")
    print(f"   Hash2: {h2}")
    
    if h1 == h2:
        print("   ✅ HASH IDENTIQUES - Score: 1.0")
        return 1.0
    else:
        print("   ❌ HASH DIFFÉRENTS - Score: 0.0")
        return 0.0

# Autres fonctions désactivées
def structure_similarity(df1, df2): return 0.0
def statistics_similarity(df1, df2): return 0.0  
def size_similarity(df1, df2): return 0.0
def quality_similarity(tests1, tests2): return 0.0

def global_similarity_optimized(current_df, current_hash, current_stats, 
                              parent_hash, parent_stats, current_tests, parent_tests):
    """Seulement hash similarity"""
    print("🎯 DEBUG - global_similarity_optimized appelée")
    return hash_similarity(current_hash, parent_hash)

def global_similarity(df1, df2, hash1, hash2, tests1, tests2):
    return hash_similarity(hash1, hash2)

def load_quality_results(result_file):
    return {}