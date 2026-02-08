from flask import Flask, render_template, Response, stream_with_context, send_from_directory, request, jsonify, make_response
import os
import shutil
import traceback
import db


# Limpieza caché
for root, dirs, files in os.walk("."):
    for d in dirs:
        if d == "__pycache__":
            shutil.rmtree(os.path.join(root, d), ignore_errors=True)

app = Flask(__name__)

try: db.init_db()
except: pass

@app.route('/')
def dashboard():
    try:
        jrv_list = db.get_all_jrvs_status()
        total = len(jrv_list)
        validated = sum(1 for x in jrv_list if x['estado'] == 'VALIDADO')
        pending = sum(1 for x in jrv_list if x['estado'] == 'PENDIENTE' and x['has_trep'] and x['has_esc'])
        stats = db.get_global_stats()
        return render_template('dashboard.html', jrv_list=jrv_list, total=total, validated=validated, pending=pending, stats=stats)
    except Exception as e:
        traceback.print_exc()
        return f"Error dashboard: {str(e)}", 500



@app.route('/comparison/<jrv>')
def comparison(jrv):
    level = request.args.get('level', 'PRESIDENTE')
    try:
        comp_data = db.get_comparison_data(jrv, nivel=level)
        nav = db.get_jrv_navigation(jrv)
        return render_template('comparison.html', jrv=jrv, comp_data=comp_data, prev_jrv=nav['prev'], next_jrv=nav['next'], level=level)
    except Exception as e:
        return f"Error comparacion: {str(e)}", 500

@app.route('/api/update_results', methods=['POST'])
def api_update_results():
    try:
        data = request.json
        action = data.get('action')
        id_acta = data.get('id_acta')
        if not id_acta: return jsonify({'success': False}), 400

        if action == 'update_vote':
            candidato = data.get('candidato') or data.get('key')
            db.update_result_vote(id_acta, candidato, int(data['votos']))
        elif action == 'update_resumen':
            db.update_resumen_field(id_acta, data.get('key'), int(data['votos']))
        elif action == 'delete_row':
            db.delete_result_row(id_acta, data['candidato'])
        elif action == 'add_row':
            db.add_result_row(id_acta, data['candidato'], int(data['votos']))
            
        return jsonify({'success': True})
    except: return jsonify({'success': False}), 500

@app.route('/api/validate_jrv', methods=['POST'])
def api_validate_jrv():
    try:
        data = request.json
        jrv_code = data.get('jrv')
        level = data.get('level', 'PRESIDENTE')
        if not jrv_code: return jsonify({'success': False}), 400
        
        # 1. Marcar actual como VALIDADA
        # Note: We need to update validate_acta_trep in db.py to support level if we want granular validation
        # For now, let's assume we validate unique combination of JRV+TREP+NIVEL?
        # But wait, db.validate_acta_trep updates WHERE jrv=? AND origen='TREP'. 
        # I should update db.py first or assume it receives level?
        # I'll update db.py to accept level in the next step or I can do it now by passing it if python allows.
        # But validate_acta_trep definition in db.py is `def validate_acta_trep(jrv):`.
        # I need to update db.py signature first!
        # So I will update db.py's validate_acta_trep in a moment.
        db.validate_acta_trep(jrv_code, nivel=level)
        
        # 2. Buscar la SIGUIENTE acta PENDIENTE de forma eficiente
        next_pending = db.get_next_pending_jrv(jrv_code)
        
        return jsonify({'success': True, 'next_jrv': next_pending})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/delete_jrv', methods=['POST'])
def api_delete_jrv():
    try:
        data = request.json
        if data.get('id_jrv'):
            db.delete_jrv_data(data.get('id_jrv'))
            return jsonify({'success': True})
        return jsonify({'success': False}), 400
    except: return jsonify({'success': False}), 500



@app.route('/data/<path:filename>')
def serve_image(filename):
    return send_from_directory('data', filename)

@app.route('/api/upload_acta', methods=['POST'])
def api_upload_acta():
    try:
        jrv = request.form.get('jrv')
        nivel = request.form.get('nivel', 'PRESIDENTE')
        source = request.form.get('source') # 'TREP' or 'OFICIAL'
        file = request.files.get('file')

        if not jrv or not source or not file:
            return jsonify({'success': False, 'message': 'Missing parameters'}), 400

        # Determine paths
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if source == 'TREP':
            upload_dir = os.path.join(base_dir, 'data', 'ACTAS', 'FRENAEL')
            filename = f"JRV_{jrv}-{nivel}.jpg" # Standardize FRENAEL format
        else: # OFICIAL
            upload_dir = os.path.join(base_dir, 'data', 'ACTAS', 'OFICIAL')
            # Standardize OFICIAL format (e.g. 1234-PRESIDENTE.jpg)
            filename = f"{jrv}-{nivel}.jpg"

        os.makedirs(upload_dir, exist_ok=True)
        filepath = os.path.join(upload_dir, filename)
        
        # Save File
        file.save(filepath)

        # Update DB
        rel_path = f"data/ACTAS/{'FRENAEL' if source == 'TREP' else 'OFICIAL'}/{filename}"
        db.register_manual_upload(jrv, nivel, source, rel_path)

        return jsonify({'success': True, 'filepath': rel_path})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/delete_acta', methods=['POST'])
def api_delete_acta():
    try:
        data = request.json
        jrv = data.get('jrv')
        nivel = data.get('nivel')
        source = data.get('source')

        if not jrv or not nivel or not source:
             return jsonify({'success': False, 'message': 'Missing parameters'}), 400
             
        # Call DB to delete record (and maybe file?)
        # For safety we might keep the file or rename it, but user asked to delete.
        # Let's delete the DB record first.
        filepath = db.delete_acta_record(jrv, nivel, source)
        
        # Optional: Delete file from disk if returned
        if filepath:
             full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filepath)
             if os.path.exists(full_path):
                 try:
                     # Rename to .bak instead of delete for safety
                     os.rename(full_path, full_path + ".bak")
                 except: pass

        return jsonify({'success': True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/summary_table/<level>')
def api_summary_table(level):
    try:
        data = db.get_summary_table(level)
        return jsonify(data)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("-------------------------------------------------------")
    print("FRENAEL AUDITORÍA 2025 v5.0 (Optimized)")
    print("Abre: http://127.0.0.1:5000")
    print("-------------------------------------------------------")
    app.run(debug=True, port=5000)