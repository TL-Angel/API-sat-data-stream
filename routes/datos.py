# -*- coding: utf-8 -*-
__author__ = "David Pastrana"
__copyright__ = "Copyright 2022, Robina"
__credits__ = ["David Pastrana"]
__license__ = "GPL"
__version__ = "2.0.0"
__email__ = "losphiereth@gmail.com"
__status__ = "Development"

from datetime import datetime, timedelta
from typing import Optional
from xmlrpc.client import Boolean
from fastapi import APIRouter

from config.cnn import MONGO_CLIENT

datos_routes = APIRouter(prefix="/data", tags=["Datos"])

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
@datos_routes.post('/sales/{rfc_emisor}')
async def get_ventas(rfc_emisor: str, 
    startDate: Optional[datetime] = None, 
    endDate: Optional[datetime] = None, 
    includeTaxes: Optional[Boolean] = True):
    """
    Obtiene la información de las ventas (facturas con tipo de comprobante I) de un determinado período de tiempo
    """
    if not endDate:
        endDate = datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)
    
    startMonth = 1 if endDate.month == 12 else endDate.month + 1
    startYear = endDate.year if startMonth == 1 else endDate.year - 1
    if not startDate:
        startDate = datetime(startYear, startMonth, 1) # últimos 12 meses incluyendo el mes actual
    
    condFactorTraslados = 1 if not includeTaxes else {
                '$cond': {
                    'if': { '$and': [ { '$eq': ['$cfdi:comprobante.tipodecomprobante','P']},{ '$gt':[ { '$toDouble': '$ppd_original.cfdi:impuestos.totalimpuestostrasladados'}, 0 ] } ] },
                    'then': { '$sum':[ 1, { '$divide': [ { '$toDouble':'$ppd_original.cfdi:impuestos.totalimpuestostrasladados'}, { '$subtract':[ { '$toDouble':'$ppd_original.cfdi:comprobante.total' }, {'$toDouble':'$ppd_original.cfdi:impuestos.totalimpuestostrasladados'} ] } ] } ] },
                    'else': 1
                }
            }
    
    campoMontoCFDI = '$cfdi:comprobante.subtotal' if not includeTaxes else '$cfdi:comprobante.total'

    pipeline = [
        { '$match': { 'cfdi:emisor.rfc': '{0}'.format(rfc_emisor) } },
        { '$lookup': { 'from': 'metadata_emitidas', 'localField': 'cfdi:complemento.tfd:timbrefiscaldigital.uuid', 'foreignField': 'Uuid', 'as': 'meta_info' } },
        { '$unwind': '$meta_info' },
        { '$unwind': { 'path': '$cfdi:complemento.pago10:pagos.pago10:pago', 'preserveNullAndEmptyArrays': True } },
        { '$unwind': { 'path': '$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado', 'preserveNullAndEmptyArrays': True } },
        { '$set': { 'iddocumento_lc': { '$toLower': '$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado.iddocumento' } } },
        { '$lookup': { 'from': 'xml_emitidas', 'localField': 'iddocumento_lc', 'foreignField': 'cfdi:complemento.tfd:timbrefiscaldigital.uuid', 'as': 'ppd_original' } },
        { '$unwind': { 'path': '$ppd_original', 'preserveNullAndEmptyArrays': True } }, 
        { '$set': { 
            'fecha_final': { 
                '$cond': {
                    'if':{ '$and': [ { '$eq': ['$cfdi:comprobante.tipodecomprobante','P' ] },{ '$gt': ['$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado',None ] } ] },
                    'then': '$ppd_original.cfdi:comprobante.fecha',
                    'else': '$cfdi:comprobante.fecha'
                }
            },
            'factor_traslados': condFactorTraslados
        } },
        { '$match': {
            '$and': [
                { 'fecha_final': { '$gte': startDate } },
                { 'fecha_final': { '$lt': endDate } },
            ],
        } },
        # Agrupar tomando el min del saldo insoluto y la suma de los pagos por cada docto. relacionado, 
        # por cada pago, por cada factura
        { '$group': {
            '_id': { '_id': '$_id',
                'rfc_emisor': '$cfdi:emisor.rfc',
                'cfdi:comprobante': '$cfdi:comprobante',
                'cfdi:complemento': '$cfdi:complemento',
                'estatus': '$meta_info.Estatus',
                'fecha_final': '$fecha_final',
                'iddocumento_lc': '$iddocumento_lc' },
            'pago': { '$sum': {
                '$switch': {
                    'branches': [
                        # Escenario donde el comprobante es tipo P, pero el método de pago original es PUE: 
                        # No tomar este pago, es una captura errónea; 
                        # se sumará el subtotal del comprobante original tipo I con método de pago PUE,
                        # de acuerdo con la instrucción del contador de que los pagos PUE deben 
                        # considerarse pagados en la fecha en que son emitidos.
                        { 'case': { '$and': [ { '$eq':['$cfdi:comprobante.tipodecomprobante', 'P'] },
                                        { '$eq': ['$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado.metododepagodr', 'PUE']}] }, 
                        'then': 0 },
                        { 'case': { '$and': [ { '$eq':['$cfdi:comprobante.tipodecomprobante','P'] },
                                        { '$gt': ['$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado', None ] } ] },
                        'then': { '$cond': {
                            'if':{ '$eq': ['$cfdi:complemento.pago10:pagos.pago10:pago.monedap','MXN'] },
                            'then': { '$round': [ { '$divide': [ { '$toDouble':'$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado.imppagado' }, '$factor_traslados' ] }, 2 ] },
                            'else': { '$round': [ { '$multiply': [ { '$toDouble':'$cfdi:complemento.pago10:pagos.pago10:pago.pago10:doctorelacionado.imppagado' }, { '$toDouble':'$cfdi:complemento.pago10:pagos.pago10:pago.tipocambiop' } ] }, 2 ] } }
                        } },
                    ], 'default': 0
                }
            } }
        } },
        { '$project': {
            '_id': '$_id._id',
            'rfc_emisor': '$_id.rfc_emisor',
            'cfdi:comprobante': '$_id.cfdi:comprobante',
            'cfdi:complemento': '$_id.cfdi:complemento',
            'estatus': '$_id.estatus',
            'fecha_final': '$_id.fecha_final',
            'iddocumento_lc': '$_id.iddocumento_lc',
            'pago': 1, 'saldo': 1,
        } },
        # Agrupar a nivel de año
        { '$group': {
            '_id': { 'rfc_emisor': '$rfc_emisor' },
            'ingreso_vigente': { '$sum': {
                '$switch': {
                    'branches': [
                        { 'case': { '$and': [ { '$eq': ['$cfdi:comprobante.tipodecomprobante','I'] }, { '$eq': ['$estatus', 1] }, {'$not': { '$in':['$cfdi:comprobante.moneda',['MXN','XXX']] } } ] }, 'then': { '$multiply': [ { '$toDouble': campoMontoCFDI }, { '$toDouble': '$cfdi:comprobante.tipocambio' } ] } },
                        { 'case': { '$and': [ { '$eq': ['$cfdi:comprobante.tipodecomprobante','I'] }, { '$eq': ['$estatus', 1] } ] }, 'then': { '$toDouble': campoMontoCFDI } }
                    ], 'default': 0
                }
            } },
        } },
        { '$project': {
            '_id': 0,
            'rfc_emisor': '$_id.rfc_emisor',
            'fecha_inicio': startDate,
            'fecha_fin': endDate,
            'ventas_totales':'$ingreso_vigente',
        } }
    ]

    return list(MONGO_CLIENT.db_sat['xml_emitidas'].aggregate(pipeline))[0]
