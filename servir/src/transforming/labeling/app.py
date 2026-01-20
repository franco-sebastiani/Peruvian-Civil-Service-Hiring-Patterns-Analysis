"""
Streamlit web interface for labeling experience data.

Run with: streamlit run servir/src/transforming/labeling/app.py
"""

import streamlit as st
import json
from pathlib import Path


# File paths
DATA_DIR = Path(__file__).parent / "data"
SAMPLES_FILE = DATA_DIR / "samples.json"
LABELED_FILE = DATA_DIR / "labeled.json"


def load_samples():
    """Load samples from JSON file."""
    if not SAMPLES_FILE.exists():
        st.error(f"Samples file not found: {SAMPLES_FILE}")
        st.info("Run sampler.py first to generate samples.")
        st.stop()
    
    with open(SAMPLES_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_labeled_data(samples):
    """Save labeled data to JSON file."""
    labeled = [s for s in samples if s['labeled']]
    
    with open(LABELED_FILE, 'w', encoding='utf-8') as f:
        json.dump(labeled, f, indent=2, ensure_ascii=False)
    
    return len(labeled)


def main():
    st.set_page_config(page_title="Experience Labeling Tool", layout="wide")
    
    st.title("📝 Experience Requirements Labeling Tool")
    st.markdown("---")
    
    # Load data
    if 'samples' not in st.session_state:
        st.session_state.samples = load_samples()
        st.session_state.current_idx = 0
    
    samples = st.session_state.samples
    total = len(samples)
    labeled_count = sum(1 for s in samples if s['labeled'])
    
    # Progress bar
    st.sidebar.header("📊 Progress")
    st.sidebar.progress(labeled_count / total)
    st.sidebar.metric("Labeled", f"{labeled_count}/{total}")
    st.sidebar.metric("Remaining", f"{total - labeled_count}")
    
    # Navigation
    st.sidebar.header("🧭 Navigation")
    
    col1, col2 = st.sidebar.columns(2)
    if col1.button("⬅️ Previous", disabled=st.session_state.current_idx == 0):
        st.session_state.current_idx -= 1
        st.rerun()
    
    if col2.button("➡️ Next", disabled=st.session_state.current_idx >= total - 1):
        st.session_state.current_idx += 1
        st.rerun()
    
    jump_to = st.sidebar.number_input(
        "Jump to sample:", 
        min_value=1, 
        max_value=total, 
        value=st.session_state.current_idx + 1
    )
    if st.sidebar.button("Go"):
        st.session_state.current_idx = jump_to - 1
        st.rerun()
    
    # Current sample
    idx = st.session_state.current_idx
    sample = samples[idx]
    
    st.header(f"Sample {idx + 1} of {total}")
    
    # Show context
    col1, col2 = st.columns(2)
    col1.metric("Job Title", sample['job_title'])
    col2.metric("Institution", sample['institution'])
    
    st.markdown("---")
    
    # Original text
    st.subheader("📄 Original Experience Text")
    st.text_area(
        "Full text:",
        value=sample['experience_text'],
        height=150,
        disabled=True,
        key=f"original_{idx}"
    )
    
    st.markdown("---")
    
    # Labeling section
    st.subheader("✏️ Label the Experience")
    st.info("Extract the GENERAL and SPECIFIC experience portions with their keywords. Leave blank if not present.")
    
    # General Experience Section
    st.markdown("### 🔹 General Experience")
    general_desc = st.text_area(
        "**Description** (full text with years)",
        value=sample.get('experience_general_description', ''),
        height=80,
        key=f"general_desc_{idx}",
        placeholder="e.g., 'DOS (02) AÑOS EN EL SECTOR PÚBLICO O PRIVADO'"
    )
    
    general_keywords = st.text_input(
        "**Keywords** (comma-separated key terms)",
        value=', '.join(sample.get('experience_general_keywords', [])) if sample.get('experience_general_keywords') else '',
        key=f"general_keywords_{idx}",
        placeholder="e.g., 'SECTOR PÚBLICO, SECTOR PRIVADO, EXPERIENCIA LABORAL'"
    )
    
    st.markdown("---")
    
    # Specific Experience Section
    st.markdown("### 🔹 Specific Experience")
    specific_desc = st.text_area(
        "**Description** (full text with years)",
        value=sample.get('experience_specific_description', ''),
        height=80,
        key=f"specific_desc_{idx}",
        placeholder="e.g., 'UN (01) AÑO EN SANEAMIENTO FÍSICO LEGAL'"
    )
    
    specific_keywords = st.text_input(
        "**Keywords** (comma-separated key terms)",
        value=', '.join(sample.get('experience_specific_keywords', [])) if sample.get('experience_specific_keywords') else '',
        key=f"specific_keywords_{idx}",
        placeholder="e.g., 'SANEAMIENTO, FÍSICO LEGAL, SECTOR PÚBLICO'"
    )
    
    st.markdown("---")
    
    # Save button
    col1, col2, col3 = st.columns([1, 1, 2])
    
    if col1.button("💾 Save", type="primary", use_container_width=True):
        # Process keywords into lists
        general_kw_list = [k.strip() for k in general_keywords.split(',') if k.strip()]
        specific_kw_list = [k.strip() for k in specific_keywords.split(',') if k.strip()]
        
        # Save to sample
        samples[idx]['experience_general_description'] = general_desc if general_desc.strip() else None
        samples[idx]['experience_general_keywords'] = general_kw_list if general_kw_list else []
        samples[idx]['experience_specific_description'] = specific_desc if specific_desc.strip() else None
        samples[idx]['experience_specific_keywords'] = specific_kw_list if specific_kw_list else []
        samples[idx]['labeled'] = True
        
        # Save to file
        saved_count = save_labeled_data(samples)
        
        st.success(f"✓ Saved! ({saved_count} total labeled)")
        
        # Auto-advance to next
        if idx < total - 1:
            st.session_state.current_idx += 1
            st.rerun()
    
    if col2.button("⏭️ Skip", use_container_width=True):
        if idx < total - 1:
            st.session_state.current_idx += 1
            st.rerun()
    
    # Export button
    st.sidebar.markdown("---")
    st.sidebar.header("💾 Export")
    if st.sidebar.button("📥 Download Labeled Data"):
        save_labeled_data(samples)
        st.sidebar.success(f"Saved {labeled_count} labeled samples to:\n{LABELED_FILE}")


if __name__ == "__main__":
    main()