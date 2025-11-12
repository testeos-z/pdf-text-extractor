import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync } from 'fs';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

// Configurar cliente de Supabase
const supabaseUrl = "";
const supabaseKey = "";

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Error: Las variables de entorno SUPABASE_URL y SUPABASE_KEY son requeridas');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// Configuración
const API_URL = 'http://localhost:3000/process-text';

async function vectorizeIADBDocuments() {
  try {
    console.log('🚀 Iniciando vectorización de documentos del IADB...\n');
    
    // Leer los IDs de coincidencias exactas
    console.log('📖 Leyendo IDs de documentos del IADB...');
    const exactMatchIds = JSON.parse(readFileSync('./exact_match_ids_simple.json', 'utf8'));
    console.log(`✅ ${exactMatchIds.length} IDs cargados`);
    
    // Leer los datos completos de documentos del IADB
    console.log('📖 Leyendo datos completos de documentos del IADB...');
    const documentData = JSON.parse(readFileSync('./documentos_iadb_completo.json', 'utf8'));
    console.log(`✅ ${documentData.documents.length} documentos cargados`);
    
    // Crear un mapa de documentos por ID para búsqueda rápida
    const documentMap = new Map();
    documentData.documents.forEach(doc => {
      documentMap.set(doc.originalName.replace('.pdf', ''), doc);
    });
    
    console.log('🔍 Creando mapa de documentos...');
    
    // Obtener archivos del bucket de Supabase
    console.log('📂 Obteniendo archivos del bucket de Supabase...');
    const { data: bucketFiles, error: bucketError } = await supabase.storage
      .from('vector')
      .list('uploaded', {
        limit: 100000,
        offset: 0
      });
    
    if (bucketError) {
      console.error('❌ Error al obtener archivos del bucket:', bucketError.message);
      return;
    }
    
    console.log(`✅ ${bucketFiles.length} archivos encontrados en el bucket`);
    
    // Crear mapa de archivos del bucket por ID
    const bucketMap = new Map();
    bucketFiles.forEach(file => {
      bucketMap.set(file.id, file);
    });
    
    // Filtrar solo los archivos que coinciden con nuestros IDs
    const filesToProcess = exactMatchIds.filter(id => bucketMap.has(id));
    console.log(`📊 Archivos a procesar: ${filesToProcess.length} de ${exactMatchIds.length}`);
    
    if (filesToProcess.length === 0) {
      console.log('❌ No se encontraron archivos para procesar');
      return;
    }
    
    // Procesar cada archivo de uno en uno
    const results = [];
    let processed = 0;
    let success = 0;
    let errors = 0;
    let textExtractionStats = {
      successful: 0,
      failed: 0,
      totalCharacters: 0,
      averageCharacters: 0
    };
    
    console.log('\n🔄 Iniciando procesamiento secuencial...');
    
    // Función para procesar un archivo individual
    async function processFile(fileId) {
      try {
        const file = bucketMap.get(fileId);
        const fileName = file.name;
        
        console.log(`\n📄 Procesando: ${fileName} (${processed + 1}/${filesToProcess.length})`);
        console.log(`🆔 File ID: ${fileId}`);
        console.log(`📁 Tamaño del archivo: ${file.metadata?.size || 'N/A'} bytes`);
        
        // Buscar el documento correspondiente en los datos del IADB
        const documentName = fileName.replace('.pdf', '');
        const iadbDoc = documentMap.get(documentName);
        
        if (!iadbDoc) {
          console.log(`⚠️  No se encontró información del IADB para: ${fileName}`);
          return {
            fileId,
            fileName,
            success: false,
            error: 'No se encontró información del IADB'
          };
        }
        
        // Obtener el archivo del bucket
        const { data: fileData, error: downloadError } = await supabase.storage
          .from('vector')
          .download(`uploaded/${fileName}`);
        
        if (downloadError) {
          console.log(`❌ Error descargando archivo: ${downloadError.message}`);
          return {
            fileId,
            fileName,
            success: false,
            error: `Error descargando: ${downloadError.message}`
          };
        }
        
        // Extraer texto real del PDF usando pdftotext del sistema
        console.log(`📖 Extrayendo texto del PDF: ${fileName}`);
        const arrayBuffer = await fileData.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        
        let extractedText = '';
        let extractionSuccess = false;
        
        try {
          // Guardar temporalmente el PDF para extraer texto
          const tempPath = `/tmp/${fileId}.pdf`;
          const textPath = `/tmp/${fileId}.txt`;
          require('fs').writeFileSync(tempPath, buffer);
          
          // Extraer texto usando pdftotext
          await execAsync(`pdftotext "${tempPath}" "${textPath}"`);
          
          // Leer el texto extraído
          extractedText = require('fs').readFileSync(textPath, 'utf8');
          
          // Limpiar archivos temporales
          require('fs').unlinkSync(tempPath);
          require('fs').unlinkSync(textPath);
          
          // Limpiar el texto extraído (como n8n)
          extractedText = extractedText
            .replace(/\s+/g, ' ') // Reemplazar múltiples espacios con uno solo
            .replace(/\n\s*\n/g, '\n') // Reemplazar múltiples saltos de línea
            .replace(/\r\n/g, '\n') // Normalizar saltos de línea
            .replace(/\t/g, ' ') // Reemplazar tabs con espacios
            .trim();
          
          // Verificar que se extrajo contenido válido
          if (extractedText.length > 50) {
            extractionSuccess = true;
            textExtractionStats.successful++;
            textExtractionStats.totalCharacters += extractedText.length;
            console.log(`✅ Texto extraído exitosamente: ${extractedText.length} caracteres`);
          } else {
            textExtractionStats.failed++;
            console.log(`⚠️  Texto extraído muy corto: ${extractedText.length} caracteres`);
          }
        } catch (pdfError) {
          console.log(`❌ Error extrayendo PDF: ${pdfError.message}`);
          extractionSuccess = false;
          textExtractionStats.failed++;
        }
        
        // Si la extracción falló, usar contenido básico
        if (!extractionSuccess || extractedText.length < 50) {
          extractedText = `[Error extrayendo contenido del PDF - Archivo: ${fileName}, Tamaño: ${buffer.length} bytes]`;
          console.log(`⚠️  Usando contenido básico para: ${fileName}`);
        }
        
        // Crear el texto final con metadatos estructurados (como n8n)
        const text = `TÍTULO: ${iadbDoc.cleanName}
          DOCUMENTO: ${fileName}
          URL_IADB: ${iadbDoc.url}
          URL_SUPABASE: ${supabaseUrl}/storage/v1/object/public/vector/uploaded/${fileName}
          TAMAÑO: ${buffer.length} bytes
          FECHA_PROCESAMIENTO: ${new Date().toISOString()}

          CONTENIDO:
          ${extractedText}`;
        
        // Preparar datos para la API
        const requestData = {
          text: {
            name: fileName,
            id: fileId,
            title: iadbDoc.cleanName,
            content: text,
            size: file.metadata?.size || 0,
            created: file.created_at
          },
          namespace: '*',
          flow: '@',
        };
        
        // Llamar a la API de vectorización
        const response = await fetch(API_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(requestData)
        });
        
        if (!response.ok) {
          const errorText = await response.text();
          console.log(`❌ Error en API: ${response.status} - ${errorText}`);
          return {
            fileId,
            fileName,
            success: false,
            error: `API Error: ${response.status} - ${errorText}`
          };
        }
        
        const result = await response.json();
        
        if (result.success) {
          console.log(`✅ Procesado exitosamente: ${fileName}`);
          if (result.skipped) {
            console.log(`⚠️  Documento ya existía: ${result.existingChunks} chunks`);
          } else {
            console.log(`📊 Chunks creados: ${result.results?.length || 0}`);
            console.log(`📄 Tamaño del texto procesado: ${text.length} caracteres`);
          }
        } else {
          console.log(`❌ Error procesando: ${result.error}`);
          console.log(`📄 Archivo problemático: ${fileName} (${fileId})`);
        }
        
        return {
          fileId,
          fileName,
          success: result.success,
          skipped: result.skipped,
          chunks: result.results?.length || 0,
          error: result.error || null
        };
        
      } catch (error) {
        console.log(`❌ Error procesando ${fileId}: ${error.message}`);
        return {
          fileId,
          fileName: fileId,
          success: false,
          error: error.message
        };
      }
    }
    
    // Procesar archivos de uno en uno
    const allResults = [];
    for (const fileId of filesToProcess) {
      const result = await processFile(fileId);
      allResults.push(result);
      
      // Actualizar contadores
      processed++;
      if (result.success) {
        success++;
      } else {
        errors++;
      }
      
      // Mostrar progreso cada 10 archivos
      if (processed % 10 === 0) {
        console.log(`📊 Progreso: ${processed}/${filesToProcess.length} procesados (✅ ${success} exitosos, ❌ ${errors} errores)`);
      }
      
      // Pequeña pausa entre archivos para no sobrecargar la API
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    // Calcular estadísticas de extracción de texto
    textExtractionStats.averageCharacters = textExtractionStats.successful > 0 
      ? Math.round(textExtractionStats.totalCharacters / textExtractionStats.successful) 
      : 0;

    console.log('\n📊 RESUMEN FINAL:');
    console.log('=' * 50);
    console.log(`📋 Total de archivos procesados: ${processed}`);
    console.log(`✅ Exitosos: ${success}`);
    console.log(`❌ Errores: ${errors}`);
    console.log(`📈 Tasa de éxito: ${((success / processed) * 100).toFixed(2)}%`);
    
    console.log('\n📖 ESTADÍSTICAS DE EXTRACCIÓN DE TEXTO:');
    console.log('=' * 50);
    console.log(`✅ Extracciones exitosas: ${textExtractionStats.successful}`);
    console.log(`❌ Extracciones fallidas: ${textExtractionStats.failed}`);
    console.log(`📊 Total de caracteres extraídos: ${textExtractionStats.totalCharacters.toLocaleString()}`);
    console.log(`📈 Promedio de caracteres por documento: ${textExtractionStats.averageCharacters.toLocaleString()}`);
    console.log(`📈 Tasa de éxito de extracción: ${((textExtractionStats.successful / processed) * 100).toFixed(2)}%`);
    
    // Mostrar algunos ejemplos de errores
    const errorResults = allResults.filter(r => !r.success);
    if (errorResults.length > 0) {
      console.log('\n❌ Ejemplos de errores:');
      errorResults.slice(0, 5).forEach((result, index) => {
        console.log(`   ${index + 1}. ${result.fileName}: ${result.error}`);
      });
      
      if (errorResults.length > 5) {
        console.log(`   ... y ${errorResults.length - 5} errores más`);
      }
    }
    
    // Guardar reporte detallado
    const report = {
      summary: {
        totalFiles: processed,
        successful: success,
        errors: errors,
        successRate: ((success / processed) * 100).toFixed(2)
      },
      textExtraction: {
        successful: textExtractionStats.successful,
        failed: textExtractionStats.failed,
        totalCharacters: textExtractionStats.totalCharacters,
        averageCharacters: textExtractionStats.averageCharacters,
        successRate: ((textExtractionStats.successful / processed) * 100).toFixed(2)
      },
      results: allResults,
      generatedAt: new Date().toISOString()
    };
    
    writeFileSync('./vectorization_report.json', JSON.stringify(report, null, 2));
    console.log('\n💾 Reporte detallado guardado en: vectorization_report.json');
    
  } catch (error) {
    console.error('❌ Error durante la vectorización:', error.message);
  }
}

// Ejecutar la función
vectorizeIADBDocuments();